import datetime
import random
from pprint import pprint
from time import sleep

from celery import Celery

from helper_functions import init_db_connection, addDictToDB, convert_list_to_list_of_dicts, write_dictlist_to_db
from instagram_dataflows import get_n_sets_of_profiles

app: Celery = Celery('tasks', broker='pyamqp://guest@localhost//')

from celery.signals import worker_process_init, worker_process_shutdown

db_conn = None

@worker_process_init.connect
def init_worker(**kwargs):
    global db_conn
    print('Initializing database connection for worker.')
    db_conn = init_db_connection()


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    global db_conn
    if db_conn:
        print('Closing database connectionn for worker.')
        db_conn.close()




@app.task
def get_search_from_insta_api():
    global db_conn
    response = get_n_sets_of_profiles(1)
    x = convert_list_to_list_of_dicts(response)
    write_dictlist_to_db(db_conn, x, pg_table='instagram_users_from_search')
    return response



@app.task
def run_get_search_from_insta_api():
    global db_conn
    db_conn = init_db_connection()
    list_vals = []
    for i in range(1, 3):
        rand_val = random.randint(7, 12)
        sleep(rand_val)
        response = get_search_from_insta_api()
    return list_vals


#if __name__ == '__main__':
#    app.worker_main()