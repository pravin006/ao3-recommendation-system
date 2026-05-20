-- postgres-init/001_create_ao3_tables.sql

CREATE TABLE IF NOT EXISTS all_works (
    id TEXT PRIMARY KEY,
    published TEXT,
    link TEXT,
    status TEXT,
    title TEXT,
    author TEXT,
    rating TEXT,
    categories TEXT,
    fandoms TEXT,
    relationships TEXT,
    characters TEXT,
    freeform_tags TEXT,
    summary TEXT
);

CREATE TABLE IF NOT EXISTS my_bookmarks (
    work_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS public_bookmarks (
    user_id TEXT,
    work_id TEXT,
    PRIMARY KEY (user_id, work_id)
);

CREATE TABLE IF NOT EXISTS my_rss_links (
    link TEXT PRIMARY KEY
);