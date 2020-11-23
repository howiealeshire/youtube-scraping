import datetime
import json
import os
import random
from pprint import pprint
from time import sleep

from celery import Celery
from celery.schedules import crontab
from celery.utils.log import get_task_logger

from table_data import table_name_insta_users, table_name_posts, table_name_searched_insta_users, table_name_videos, \
    table_name_channels

logger = get_task_logger(__name__)
import runscraping as rs

from helper_functions import init_db_connection, write_dict_to_db, convert_list_to_list_of_dicts, write_dictlist_to_db, \
    grouper, load_db_table, get_dict_list_vals_for_key, load_unused_users, update_used_statuses, \
    get_channel_ids_not_in_channels, init_remote_db_connection
from instagram_dataflows import get_n_sets_of_profiles, run_scrapy
from youtube_data_flows import get_most_popular_vids, get_most_popular_channels, \
    get_channel_from_channel_id

app: Celery = Celery('tasks', broker='pyamqp://guest@localhost//')

from celery.signals import worker_process_init, worker_process_shutdown

db_conn = None
remote_db_conn = None
app.conf.timezone = 'US/Eastern'


@worker_process_init.connect
def init_worker(**kwargs):
    global db_conn
    global remote_db_conn
    print('Initializing database connection for worker.')
    db_conn = init_db_connection()
    print('Initializing remote database connection for worker.')
    remote_db_conn = init_remote_db_connection()


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    global db_conn
    global remote_db_conn
    if db_conn:
        print('Closing database connection for worker.')
        db_conn.close()
    if remote_db_conn:
        print('Closing remote database connection for worker.')
        remote_db_conn.close()


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Calls test('hello') every 10 seconds.
    # Executes every Monday morning at 7:30 a.m.
    sender.add_periodic_task(
        crontab(hour=13, minute=21),
        test.s('Happy Mondays!'),
    )


@app.task
def test(arg):
    logger.info(f'Arg: {arg}')


@app.task
def get_search_from_insta_api():
    global remote_db_conn
    response = get_n_sets_of_profiles(remote_db_conn,1)
    return response


@app.task
def get_most_popular_vids_youtube_api():
    global db_conn
    x = get_most_popular_vids(db_conn)
    if x:
        write_dictlist_to_db(db_conn, x, pg_table=table_name_videos)
    return x


@app.task
def get_most_popular_channels_youtube_api():
    global db_conn
    x = get_most_popular_channels(db_conn)
    if x:
        write_dictlist_to_db(db_conn, x, pg_table=table_name_channels)
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
    for i in range(1, 200):
        rand_val = random.randint(7, 12)
        sleep(rand_val)
        response = get_most_popular_channels_youtube_api()
        list_vals.append(response)
    return list_vals


@app.task
def get_channels_from_id_youtube_api(group):
    global db_conn
    x = get_channel_from_channel_id(db_conn,group)
    if x:
        write_dictlist_to_db(db_conn, x, pg_table=table_name_channels)
    return x


@app.task
def run_get_channels_from_id_youtube_api():
    global db_conn
    list_vals = []

    channel_ids = get_channel_ids_not_in_channels(db_conn)
    for group in grouper(channel_ids, 30, ''):
        rand_val = random.randint(7, 12)
        sleep(rand_val)
        response = get_channels_from_id_youtube_api(group)
        list_vals.append(response)
    return list_vals


@app.task
def run_get_search_from_insta_api():
    list_vals = []
    for i in range(1, 300):
        rand_val = random.randint(7, 12)
        sleep(rand_val)
        response = get_search_from_insta_api()
        # pprint(response)
        list_vals.append(response)
    return list_vals




@app.task
def run_scrape():
    global db_conn
    searched_users = load_unused_users(db_conn, table_name_searched_insta_users)
    searched_usernames = get_dict_list_vals_for_key(searched_users, 'username')
    path = 'instagram_users_to_scrape.json'
    i = 0
    for group in grouper(searched_usernames, 200, ''):
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(list(group), f)
        rand_val = random.randint(7, 12)
        sleep(rand_val)
        top_users, top_posts, filepath = run_scrapy(i)
        i += 1
        write_dictlist_to_db(db_conn, top_users, table_name_insta_users)
        update_used_statuses(db_conn, top_users)
        write_dictlist_to_db(db_conn, top_posts, table_name_posts)
        update_used_statuses(db_conn, top_posts)
        os.remove(filepath)


# if __name__ == '__main__':
#    app.worker_main()
