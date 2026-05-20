

import pendulum
from typing import Any


from airflow.sdk import dag, task, Param, get_current_context, TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "ao3_postgres"
COLLAB_PARQUET_PATH = "/shared/collab_scores.parquet"
CONTENT_PARQUET_PATH = "/shared/content_scores.parquet"

MY_USER_ID = "-1"


def create_user_work_matrix(df):
    """
    Creates a sparse user-work interaction matrix for AO3 bookmarks.

    Rows = users/bookmarkers
    Columns = AO3 works
    Values = interaction_score, usually 1 for bookmarked
    """
    import pandas as pd
    from scipy.sparse import coo_matrix

    df = df.copy()

    # Make sure IDs are strings
    df["user_id"] = df["user_id"].astype(str)
    df["work_id"] = df["work_id"].astype(str)

    # Make sure score is numeric
    df["interaction_score"] = pd.to_numeric(df["interaction_score"])

    # Remove duplicate user-work pairs
    # If duplicates exist, keep the max score
    df = (
        df.groupby(["user_id", "work_id"], as_index=False)
        ["interaction_score"]
        .max()
    )

    user_mapper = {user_id: i for i, user_id in enumerate(df["user_id"].unique())}
    work_mapper = {work_id: i for i, work_id in enumerate(df["work_id"].unique())}
    print(type(user_mapper))
    print(type(work_mapper))

    user_index = df["user_id"].map(user_mapper).values
    work_index = df["work_id"].map(work_mapper).values
    print(type(user_index))
    print(type(work_index))

    X = coo_matrix(
        (
            df["interaction_score"].values,
            (user_index, work_index)
        ),
        shape=(len(user_mapper), len(work_mapper))
    )

    return X.tocsr(), user_mapper, work_mapper


def recommend_works_collaborative_item_based(
    user_id,
    X,
    user_mapper,
    work_mapper,
    min_similarity=0.01
):
    """
    Item-based collaborative filtering using cosine similarity.

    X shape:
        users × works

    Logic:
        1. Find the works bookmarked by the target user.
        2. Treat each work as a vector of bookmark users.
        3. Compare every unread candidate work against all bookmarked works.
        4. Aggregate similarities into a collaborative score.

    Returns:
        A DataFrame with one row per unread candidate work.
    """
    import numpy as np
    import pandas as pd
    from sklearn.metrics.pairwise import cosine_similarity

    user_id = str(user_id)

    if user_id not in user_mapper:
        print(f"User ID {user_id} not found.")
        return pd.DataFrame(
            columns=[
                "work_id",
                "collab_score",
                "collab_score_sum",
                "max_similarity",
                "matched_profile_work_count",
                "matched_profile_works"
            ]
        )

    # Convert users × works into works × users
    item_user_matrix = X.T

    user_index = user_mapper[user_id]
    user_interactions = X[user_index]

    # Works already bookmarked/read by the user
    bookmarked_work_indices = user_interactions.indices
    bookmarked_work_set = set(bookmarked_work_indices)

    if len(bookmarked_work_indices) == 0:
        return pd.DataFrame(
            columns=[
                "work_id",
                "collab_score",
                "collab_score_sum",
                "max_similarity",
                "matched_profile_work_count",
                "matched_profile_works"
            ]
        )

    # All unread candidate works
    candidate_work_indices = np.array([
        i for i in range(item_user_matrix.shape[0])
        if i not in bookmarked_work_set
    ])

    if len(candidate_work_indices) == 0:
        return pd.DataFrame(
            columns=[
                "work_id",
                "collab_score",
                "collab_score_sum",
                "max_similarity",
                "matched_profile_work_count",
                "matched_profile_works"
            ]
        )

    bookmarked_matrix = item_user_matrix[bookmarked_work_indices]
    candidate_matrix = item_user_matrix[candidate_work_indices]

    # Shape:
    # candidates × bookmarked works
    similarity_matrix = cosine_similarity(
        candidate_matrix,
        bookmarked_matrix
    )

    # Ignore tiny/noisy similarity values
    similarity_matrix[similarity_matrix < min_similarity] = 0

    # Aggregate similarity signals
    collab_score_sum = similarity_matrix.sum(axis=1)

    # Mean score stays on a more comparable 0–1-ish scale
    collab_score_mean = similarity_matrix.mean(axis=1)

    max_similarity = similarity_matrix.max(axis=1)

    matched_profile_work_count = (
        similarity_matrix > 0
    ).sum(axis=1)

    index_to_work = {
        index: work_id
        for work_id, index in work_mapper.items()
    }

    recommendations = []

    for row_idx, candidate_work_index in enumerate(candidate_work_indices):
        matched_indices = np.where(similarity_matrix[row_idx] > 0)[0]

        matched_profile_works = [
            index_to_work[bookmarked_work_indices[j]]
            for j in matched_indices
        ]

        recommendations.append({
            "work_id": index_to_work[candidate_work_index],
            "collab_score": collab_score_mean[row_idx],
            "collab_score_sum": collab_score_sum[row_idx],
            "max_similarity": max_similarity[row_idx],
            "matched_profile_work_count": matched_profile_work_count[row_idx],
            "matched_profile_works": matched_profile_works
        })

    recommendations_df = pd.DataFrame(recommendations)

    recommendations_df = recommendations_df.sort_values(
        by=["collab_score_sum", "max_similarity"],
        ascending=False
    ).reset_index(drop=True)

    return recommendations_df


tag_column_scores = {
    "rating": 0.5,
    "categories": 0.6,
    "fandoms": 0.4,
    "relationships": 1,
    "characters": 0.9,
    "freeform_tags": 0.9
}


def parse_json_list(value):
    import json
    import pandas as pd
    
    if pd.isna(value) or value == "":
        return []

    values = json.loads(value)

    # clean whitespace + remove duplicates within that same cell
    cleaned = [str(v).strip() for v in values if str(v).strip()]
    return list(dict.fromkeys(cleaned))


def create_content_based_matrix(all_works_df):
    import pandas as pd
    from scipy.sparse import coo_matrix

    work_mapper = {work_id: i for i, work_id in enumerate(all_works_df["work_id"].unique())}
    work_index = all_works_df["work_id"].map(work_mapper).values

    rows = []
    cols = []
    data = []

    tag_mapper = {}
    next_tag_idx = 0

    for _, row in all_works_df.iterrows():
        work_idx = work_mapper[row["work_id"]]

        for col_name, weight in tag_column_scores.items():
            for tag in row[col_name]:
                feature_name = f"{col_name}::{tag}"

                if feature_name not in tag_mapper:
                    tag_mapper[feature_name] = next_tag_idx
                    next_tag_idx += 1

                tag_idx = tag_mapper[feature_name]

                rows.append(work_idx)
                cols.append(tag_idx)
                data.append(weight)
    
    work_tag_matrix = coo_matrix(
        (data, (rows, cols)),
        shape=(len(work_mapper), len(tag_mapper))
    )
    work_tag_matrix = work_tag_matrix.tocsr()

    return work_tag_matrix, work_mapper, tag_mapper

def recommend_works_content_based(X, all_works_df):
    import numpy as np
    from sklearn.metrics.pairwise import cosine_similarity

    bookmark_mask = all_works_df["is_bookmarked"].to_numpy() == 1
    candidate_mask = all_works_df["is_bookmarked"].to_numpy() == 0

    bookmarked_matrix = X[bookmark_mask]
    candidate_matrix = X[candidate_mask]

    user_profile = np.asarray(bookmarked_matrix.mean(axis=0))
    similarities = cosine_similarity(candidate_matrix, user_profile).ravel()

    content_recommendations = all_works_df.loc[candidate_mask, ["work_id"]].copy()
    content_recommendations["content_similarity_score"] = similarities

    return content_recommendations



@dag(
    dag_id="ao3_build_recommendations",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 5, 14, tz="Asia/Singapore"),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["ao3", "postgres", "matrix", "recommendations"],
    params={
        "path": Param(
            "update and recommend",
            type="string",
            enum=["update and recommend", "recommend"],
            title="Recommendation path",
            description=(
                "'update and recommend' rebuilds score Parquets first. "
                "'recommend' uses the existing score Parquets only."
            ),
        ),
        "top_n": Param(
            20,
            type="integer",
            minimum=1,
            maximum=200,
            title="Number of recommendations to show",
        ),
    },
)
def ao3_build_recommendations():

    @task
    def resolve_path() -> bool:
        context = get_current_context()
        path = context["params"]["path"]

        should_update = path == "update and recommend"

        # print(f"Selected path: {path}")
        # print(f"Should rebuild score Parquets: {should_update}")

        return should_update



    @task
    def create_collaborative_recommendations(should_update: bool) -> bool:
        import pandas as pd

        if not should_update:
            # print("Skipping collaborative score rebuild. Using existing Parquet.")
            return False


        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records(
            """
            SELECT work_id
            FROM my_bookmarks
            """,
        )
        my_bookmarks = pd.DataFrame(rows, columns=["work_id"])
        my_bookmarks["user_id"] = MY_USER_ID
        my_bookmarks["interaction_score"] = 1

        rows = hook.get_records(
            """
            SELECT user_id, work_id
            FROM public_bookmarks
            """,
        )
        public_bookmarks = pd.DataFrame(rows, columns=["user_id", "work_id"])
        public_bookmarks["interaction_score"] = 1
        public_bookmarks = public_bookmarks[["user_id", "work_id", "interaction_score"]]


        all_interactions = pd.concat(
            [my_bookmarks, public_bookmarks],
            ignore_index=True
        )

        X, user_mapper, work_mapper = create_user_work_matrix(all_interactions)

        collaborative_recommendations = recommend_works_collaborative_item_based(
            user_id="-1",
            X=X,
            user_mapper=user_mapper,
            work_mapper=work_mapper,
            min_similarity=0.01
        )
        
        # collaborative_recommendations['collab_score_sum_normalized'] = (
        #     collaborative_recommendations["collab_score_sum"] / collaborative_recommendations["collab_score_sum"].max()
        # )
        max_collab_score = collaborative_recommendations["collab_score_sum"].max()

        if pd.notna(max_collab_score) and max_collab_score > 0:
            collaborative_recommendations["collab_score_sum_normalized"] = (
                collaborative_recommendations["collab_score_sum"]
                / max_collab_score
            )
        else:
            collaborative_recommendations["collab_score_sum_normalized"] = 0

        collaborative_recommendations.to_parquet(COLLAB_PARQUET_PATH)
        return True
    


    @task
    def create_content_recommendations(should_update: bool) -> bool:
        import pandas as pd

        if not should_update:
            # print("Skipping content score rebuild. Using existing Parquet.")
            return False
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records(
            '''
            SELECT id, rating, categories, fandoms, relationships, characters, freeform_tags
            FROM all_works
            WHERE id IN (
                            SELECT work_id
                            FROM my_bookmarks
                        )
            AND status = 'Scraped'
            ''',
        )
        bookmarked_works = pd.DataFrame(rows, columns=["work_id", "rating", "categories", "fandoms", "relationships", "characters", "freeform_tags"])

        rows = hook.get_records(
            '''
            SELECT id, rating, categories, fandoms, relationships, characters, freeform_tags
            FROM all_works
            WHERE id NOT IN (
                            SELECT work_id
                            FROM my_bookmarks
                        )
            AND status = 'Scraped'
            ''',
        )
        not_bookmarked_works = pd.DataFrame(rows, columns=["work_id", "rating", "categories", "fandoms", "relationships", "characters", "freeform_tags"])



        copy_of_not_bookmarked_works_df = not_bookmarked_works.copy()
        for col in tag_column_scores.keys():
            copy_of_not_bookmarked_works_df[col] = copy_of_not_bookmarked_works_df[col].apply(parse_json_list)
        copy_of_not_bookmarked_works_df["is_bookmarked"] = 0

        copy_of_bookmarked_works_df = bookmarked_works.copy()
        for col in tag_column_scores.keys():
            copy_of_bookmarked_works_df[col] = copy_of_bookmarked_works_df[col].apply(parse_json_list)
        copy_of_bookmarked_works_df["is_bookmarked"] = 1

        all_works_df = pd.concat(
            [copy_of_bookmarked_works_df, copy_of_not_bookmarked_works_df],
            ignore_index=True
        )


        X, work_mapper, tag_mapper = create_content_based_matrix(all_works_df)
        content_recommendations = recommend_works_content_based(X, all_works_df)

        content_recommendations.to_parquet(CONTENT_PARQUET_PATH)
        return True

    
    @task
    def final_recommendation(collaborative_rebuilt: bool,content_rebuilt: bool,) -> None:
        import pandas as pd
        from pathlib import Path

        context = get_current_context()
        top_n = context["params"]["top_n"]

        if not Path(COLLAB_PARQUET_PATH).exists():
            raise FileNotFoundError(
                f"Collaborative scores not found at {COLLAB_PARQUET_PATH}. "
                "Run with path='update and recommend' first."
            )

        if not Path(CONTENT_PARQUET_PATH).exists():
            raise FileNotFoundError(
                f"Content scores not found at {CONTENT_PARQUET_PATH}. "
                "Run with path='update and recommend' first."
            )

        print(f"Collaborative scores rebuilt this run: {collaborative_rebuilt}")
        print(f"Content scores rebuilt this run: {content_rebuilt}")

        collab_recommendations = pd.read_parquet(COLLAB_PARQUET_PATH)
        content_recommendations = pd.read_parquet(CONTENT_PARQUET_PATH)

        if "id" in content_recommendations.columns:
            content_recommendations = content_recommendations.rename(
                columns={"id": "work_id"}
            )

        if "id" in collab_recommendations.columns:
            collab_recommendations = collab_recommendations.rename(
                columns={"id": "work_id"}
            )

        final_recommendations = pd.merge(
            content_recommendations,
            collab_recommendations,
            on="work_id",
            how="left",
        )

        final_recommendations["collab_score_sum_normalized"] = (
            final_recommendations["collab_score_sum_normalized"].fillna(0)
        )

        final_recommendations["content_similarity_score"] = (
            final_recommendations["content_similarity_score"].fillna(0)
        )

        final_recommendations["final_score"] = (
            final_recommendations["content_similarity_score"] * 0.6
            + final_recommendations["collab_score_sum_normalized"] * 0.4
        )

        final_recommendations = final_recommendations.sort_values(
            "final_score",
            ascending=False,
        ).head(top_n)


        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        rows = hook.get_records(
            """
            SELECT id, published, link, title, author
            FROM all_works
            WHERE id = ANY(%s);
            """,
            parameters=(final_recommendations["work_id"].tolist(),),
        )
        recommended_works_data = pd.DataFrame(rows, columns=["id", "published", "link", "title", "author"])
        final_recommendations = pd.merge(
            final_recommendations,
            recommended_works_data,
            left_on="work_id",
            right_on="id",
            how="left",
        )

        print("Top recommendations:")
        print(
            final_recommendations[
                [
                    "work_id",
                    "title",
                    "author",
                    "content_similarity_score",
                    "collab_score_sum_normalized",
                    "final_score",
                    "published",
                    "link"
                ]
            ]
            .to_string(index=False)
        )


    should_update = resolve_path()
    
    collaborative_rebuilt = create_collaborative_recommendations(
        should_update
    )
    content_rebuilt = create_content_recommendations(
        should_update
    )

    final_recommendation(
        collaborative_rebuilt,
        content_rebuilt,
    )


ao3_build_recommendations()