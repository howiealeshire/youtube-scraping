from helper_functions import init_db_connection
from tasks import run_get_search_from_insta_api, run_get_most_popular_vids_youtube_api, \
    run_get_most_popular_channels_youtube_api

if __name__ == "__main__":
    run_get_most_popular_channels_youtube_api.delay()

