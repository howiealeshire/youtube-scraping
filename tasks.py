import datetime
import random
from pprint import pprint
from time import sleep

from celery import Celery

from helper_functions import init_db_connection, write_dict_to_db, convert_list_to_list_of_dicts, write_dictlist_to_db
from instagram_dataflows import get_n_sets_of_profiles
from youtube_data_flows import get_most_popular_vids, get_most_popular_channels

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
        print('Closing database connection for worker.')
        db_conn.close()


@app.task
def get_search_from_insta_api():
    global db_conn
    response = get_n_sets_of_profiles(1)
    x = convert_list_to_list_of_dicts(response)
    write_dictlist_to_db(db_conn, x, pg_table='instagram_users_from_search')
    return response


@app.task
def get_most_popular_vids_youtube_api(regionCode):
    global db_conn
    x = get_most_popular_vids(regionCode)
    if x:
        write_dictlist_to_db(db_conn, x, pg_table='videos')
    return x


@app.task
def get_most_popular_channels_youtube_api():
    global db_conn
    x = get_most_popular_channels()
    if x:
        write_dictlist_to_db(db_conn, x, pg_table='channels')
    return x


@app.task
def run_get_most_popular_vids_youtube_api():
    country_codes = ['DZ',
                     'AR',
                     'AU',
                     'AT',
                     'AZ',
                     'BH',
                     'BD',
                     'BY',
                     'BE',
                     'BO',
                     'BA',
                     'BR',
                     'BG',
                     'CA',
                     'CL',
                     'CO',
                     'CR',
                     'HR',
                     'CY',
                     'CZ',
                     'DK',
                     'DO',
                     'EC',
                     'EG',
                     'SV',
                     'EE',
                     'FI',
                     'FR',
                     'GE',
                     'DE',
                     'GH',
                     'GR',
                     'GT',
                     'HN',
                     'HK',
                     'HU',
                     'IS',
                     'IN',
                     'ID',
                     'IQ',
                     'IE',
                     'IL',
                     'IT',
                     'JM',
                     'JP',
                     'JO',
                     'KZ',
                     'KE',
                     'KW',
                     'LV',
                     'LB',
                     'LY',
                     'LI',
                     'LT',
                     'LU',
                     'MY',
                     'MT',
                     'MX',
                     'ME',
                     'MA',
                     'NP',
                     'NL',
                     'NZ',
                     'NI',
                     'NG',
                     'MK',
                     'NO',
                     'OM',
                     'PK',
                     'PA',
                     'PG',
                     'PY',
                     'PE',
                     'PH',
                     'PL',
                     'PT',
                     'PR',
                     'QA',
                     'RO',
                     'RU',
                     'SA',
                     'SN',
                     'RS',
                     'SG',
                     'SK',
                     'SI',
                     'ZA',
                     'KR',
                     'ES',
                     'LK',
                     'SE',
                     'CH',
                     'TW',
                     'TZ',
                     'TH',
                     'TN',
                     'TR',
                     'UG',
                     'UA',
                     'AE',
                     'GB',
                     'US',
                     'UY',
                     'VE',
                     'VN',
                     'YE',
                     'ZW']
    list_vals = []
    for i in country_codes:
        rand_val = random.randint(7, 12)
        sleep(rand_val)
        response = get_most_popular_vids_youtube_api(i)
        list_vals.append(response)
    return list_vals


@app.task
def run_get_most_popular_channels_youtube_api():
    list_vals = []
    for i in range(1, 26):
        rand_val = random.randint(7, 12)
        sleep(rand_val)
        response = get_most_popular_channels_youtube_api()
        list_vals.append(response)
    return list_vals


@app.task
def run_get_search_from_insta_api():
    list_vals = []
    for i in range(1, 200):
        rand_val = random.randint(7, 12)
        sleep(rand_val)
        response = get_search_from_insta_api()
        pprint(response)
        list_vals.append(response)
    return list_vals

# if __name__ == '__main__':
#    app.worker_main()
