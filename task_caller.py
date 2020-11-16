from helper_functions import init_db_connection
from celery import group
from tasks import run_get_search_from_insta_api, run_get_most_popular_vids_youtube_api, \
    run_get_most_popular_channels_youtube_api, run_get_channels_from_id_youtube_api, setup_periodic_tasks, run_scrape

if __name__ == "__main__":
    group(run_get_most_popular_vids_youtube_api.delay(),
          run_get_channels_from_id_youtube_api.delay(),
          run_get_search_from_insta_api.delay(),
          run_get_most_popular_channels_youtube_api.delay())
          #run_scrape.delay())
    #run_scrape.delay()

    #run_get_most_popular_channels_youtube_api.delay()
    #run_get_most_popular_vids_youtube_api.delay()
    #run_get_search_from_insta_api.delay()
    #run_get_channels_from_id_youtube_api.delay()
    # setup_periodic_tasks()
    # run_get_most_popular_vids_youtube_api.delay() CHECK
    # run_get_most_popular_channels_youtube_api.delay() CHECK
    #run_scrape.delay() CHECK
