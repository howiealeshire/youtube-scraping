from helper_functions import init_db_connection
from tasks import  run_get_search_from_insta_api

if __name__ == "__main__":
    run_get_search_from_insta_api.delay()

