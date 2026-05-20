#!/usr/bin/env bash

# -e: stop if a command fails
# -u: stop if an undefined variable is used
# pipefail: treat failures inside piped commands as errors
set -euo pipefail

# if number of arguments given are less than 1, print usage and exit
if [ "$#" -lt 1 ]; then
  echo "Usage example:"
  echo '''bash ./scripts/add_rss_link.sh "https://archiveofourown.org/tags/86392389/feed.atom" "rss_link_2" "rss_link_3"'''
  exit 1
fi

for RSS_LINK in "$@"; do
    echo "Adding RSS link: $RSS_LINK"
    docker compose exec -T ao3-postgres \
        psql \
        -U ao3_user \
        -d ao3_db \
        -c "INSERT INTO my_rss_links (link) VALUES ('$RSS_LINK') ON CONFLICT (link) DO NOTHING;"
done

echo "Done."
