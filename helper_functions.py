import codecs
import copy
import csv
import itertools
import json
import random
import sys
from datetime import datetime
from os import listdir
from os.path import basename, isfile, join
from pprint import pprint
from typing import Dict, List, Any, Optional

import googleapiclient.discovery
import psycopg2
import psycopg2.extras
from instagram_private_api import Client, ClientCookieExpiredError, ClientLoginRequiredError, ClientLoginError, \
    ClientError
from pypika import Table, Query, PostgreSQLQuery
import os

from hypothesis import given
from hypothesis.strategies import text

from table_data import table_name_searched_insta_users, table_name_channels, table_name_videos, table_name_words_used, \
    table_name_yt_api_keys, table_name_insta_users


def get_filtered_words() -> List[str]:
    with open('clean_words.txt') as f:
        return f.readlines()


def get_country_codes() -> List[str]:
    with open('valid_country_codes.json', 'r', encoding='utf-8') as f:
        return json.load(f)


def _to_json(python_object):
    if isinstance(python_object, bytes):
        return {'__class__': 'bytes',
                '__value__': codecs.encode(python_object, 'base64').decode()}
    raise TypeError(repr(python_object) + ' is not JSON serializable')


def _from_json(json_object):
    if '__class__' in json_object and json_object['__class__'] == 'bytes':
        return codecs.decode(json_object['__value__'].encode(), 'base64')
    return json_object


def _onlogin_callback(api, new_settings_file):
    cache_settings = api.settings
    with open(new_settings_file, 'w') as outfile:
        json.dump(cache_settings, outfile, default=_to_json)
        print('SAVED: {0!s}'.format(new_settings_file))




def init_insta_api(proxy=""):
    user_name = 'devdevdevdevdev123455667778989'
    password = 'hGC%\$;Q$J^2q3!]'
    device_id = None
    api = None
    try:

        settings_file = "instagram_login_settings.json"
        if not os.path.isfile(settings_file):
            # settings file does not exist
            print('Unable to find file: {0!s}'.format(settings_file))

            # login new
            if proxy != "":
                api = Client(
                    user_name, password,
                    on_login=lambda x: _onlogin_callback(x, settings_file),
                    proxy=proxy)
            else:
                api = Client(
                    user_name, password,
                    on_login=lambda x: _onlogin_callback(x, settings_file))
        else:
            with open(settings_file) as file_data:
                cached_settings = json.load(file_data, object_hook=_from_json)
            print('Reusing settings: {0!s}'.format(settings_file))

            device_id = cached_settings.get('device_id')
            # reuse auth settings
            if proxy != '':
                api = Client(
                    user_name, password,
                    settings=cached_settings, proxy=proxy)
            else:
                api = Client(
                    user_name, password,
                    settings=cached_settings)


    except (ClientCookieExpiredError, ClientLoginRequiredError) as e:
        print('ClientCookieExpiredError/ClientLoginRequiredError: {0!s}'.format(e))

        # Login expired
        # Do relogin but use default ua, keys and such
        if proxy != '':
            api = Client(
                user_name, password,
                device_id=device_id,
                on_login=lambda x: _onlogin_callback(x, settings_file),
                proxy=proxy)
        else:
            api = Client(
                user_name, password,
                device_id=device_id,
                on_login=lambda x: _onlogin_callback(x, settings_file))


    except ClientLoginError as e:
        print('ClientLoginError {0!s}'.format(e))
        exit(9)
    except ClientError as e:
        print('ClientError {0!s} (Code: {1:d}, Response: {2!s})'.format(e.msg, e.code, e.error_response))
        exit(9)
    except Exception as e:
        print('Unexpected Exception: {0!s}'.format(e))
        exit(99)

        # Show when login expires
    cookie_expiry = api.cookie_jar.auth_expires
    print('Cookie Expiry: {0!s}'.format(datetime.fromtimestamp(cookie_expiry).strftime('%Y-%m-%dT%H:%M:%SZ')))
    results = api.user_feed('2054964158')
    assert len(results.get('items', [])) > 0

    print('All ok')
    return api


def init_yt_client(conn):
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_key = get_unused_yt_api_key_from_db(conn)
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "client_secret_1080473066558-rh5ihim77tc3qbpvparpjnts926tuk3t.apps.googleusercontent.com.json"

    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube


def load_db_table_unexported(conn, table_name) -> List[Dict]:
    channels = Table(table_name)
    q = PostgreSQLQuery.from_(channels).select(channels.star).where(channels.exported_already != True)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q.get_sql())
    ans = cur.fetchall()
    ans1 = []
    for row in ans:
        ans1.append(dict(row))

    # cursor = conn.cursor()
    # cursor.execute(q.get_sql())
    # rows = cursor.fetchall()
    return ans1


def load_db_table_unused_yt_api_keys(conn) -> List[Dict]:
    channels = Table(table_name_yt_api_keys)
    q = PostgreSQLQuery.from_(channels).select(channels.star).where(channels.used_today != True)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q.get_sql())
    ans = cur.fetchall()
    ans1 = []
    for row in ans:
        ans1.append(dict(row))

    # cursor = conn.cursor()
    # cursor.execute(q.get_sql())
    # rows = cursor.fetchall()
    return ans1


def get_yt_api_keys_from_file(file_path: str) -> List[str]:
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.readlines()


def append_yt_api_key(api_key: str, file_path: str):
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(api_key)


def write_yt_api_keys(api_keys: List[str], file_path: str):
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(api_keys)


def get_unused_yt_api_key(unused_keys_path: str, used_file_path: str) -> str:
    unused_keys = get_yt_api_keys_from_file(unused_keys_path)
    if unused_keys:
        unused_key = unused_keys[0]
        append_yt_api_key(unused_key, used_file_path)
        return unused_key
    else:
        reset_api_keys_used(unused_keys_path, used_file_path)
        return get_unused_yt_api_key(unused_keys_path, used_file_path)


def get_unused_yt_api_key_from_db(conn) -> Optional[str]:
    api_keys = load_db_table_unused_yt_api_keys(conn)
    if api_keys:
        api_dict1 = api_keys[0]
        api_key1 = api_dict1.get('api_key')
        return api_key1
    else:
        return None


def reset_api_keys_used(unused_file_path: str, used_file_path: str):
    used_keys = get_yt_api_keys_from_file(used_file_path)
    write_yt_api_keys(used_keys, unused_file_path)
    open(used_file_path, 'w').close()


def file_len(fname):
    with open(fname, encoding='utf-8') as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def get_used_words(used_words_path: str):
    with open(used_words_path, 'r', encoding='utf-8') as f:
        return f.readlines()


def get_unused_words(unused_words_path: str):
    with open(unused_words_path, 'r', encoding='utf-8') as f:
        return f.readlines()


def get_unused_word(unused_words_path: str):
    unused_words = get_unused_words(unused_words_path)
    return unused_words[0]


def append_used_word_to_file(used_word, used_word_file):
    append_yt_api_key(used_word, used_word_file)


def check_if_in_db(conn, table_name, column, value) -> bool:
    unexported_rows = load_db_table_unexported(conn, table_name)
    db_values = [d[column] for d in unexported_rows]
    if value in db_values: return True
    return False


def is_in_dictlist_column(dict_list: List[Dict], column_name, value) -> bool:
    """@safe"""
    db_values = get_dict_list_vals_for_key(dict_list, column_name)
    if value in db_values: return True
    return False


def get_n_users_from_db_and_write_to_json(conn, path, n):
    searched_users = load_db_table(conn, table_name_searched_insta_users)
    searched_usernames = get_dict_list_vals_for_key(searched_users, 'username')[:n]
    # pprint(searched_usernames)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(list(searched_usernames), f)


def write_json_pprint(data, path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)


def get_unexported_vals_from_list(vals: List[str], column_name: str, db_vals: List[Dict]) -> List[Any]:
    unexported_vals = []
    for elem in vals:
        if not is_in_dictlist_column(db_vals, column_name, elem):
            unexported_vals.append(elem)
    return unexported_vals


def get_dict_list_vals_for_key(dict_list: List[Dict], key_name: str) -> List[Any]:
    return [d[key_name] for d in dict_list]


def load_db_table(conn, table_name) -> List[Dict]:
    channels = Table(table_name)
    q = PostgreSQLQuery.from_(channels).select(channels.star)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q.get_sql())
    ans = cur.fetchall()
    ans1 = []
    for row in ans:
        ans1.append(dict(row))

    return ans1


def load_unused_users(conn, table_name) -> List[Dict]:
    channels = Table(table_name)
    q = PostgreSQLQuery.from_(channels).select(channels.username, channels.has_been_used).where(
        channels.has_been_used == False)
    # pprint(q)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q.get_sql())
    ans = cur.fetchall()
    ans1 = []
    for row in ans:
        ans1.append(dict(row))

    return ans1

def get_users_searched_already_in_db():

    pass

def read_csv_into_dictlist(file_path):
    with open(file_path, encoding='utf-8') as f:
        a = [{k: v for k, v in row.items()}
             for row in csv.DictReader(f, skipinitialspace=True)]

    return a


def append_dictlist_to_csv(dict_list, file_name):
    if (len(dict_list) > 0):
        with open(file_name, 'a', encoding='utf8', newline='') as output_file:
            fc = csv.DictWriter(output_file,
                                fieldnames=dict_list[0].keys(),

                                )
            fc.writerows(dict_list)
    else:
        print("No items to write.")
        # sys.exit()


def write_dictlist_to_csv(dict_list, file_name):
    if (len(dict_list) > 0):
        with open(file_name, 'w', encoding='utf8', newline='') as output_file:
            fc = csv.DictWriter(output_file,
                                fieldnames=dict_list[0].keys(),

                                )
            fc.writeheader()
            fc.writerows(dict_list)
    else:
        print("No items to write. Aborting.")
        sys.exit()


def convert_list_to_list_of_dicts(l: List[str]):
    dict_list = []
    for elem in l:
        dict_list.append({'username': elem})
    return dict_list


def init_db_connection() -> Any:
    """this returns a db connection"""
    connection = psycopg2.connect(user="postgres",
                                  password="test",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")
    return connection


def init_test_db_connection() -> Any:
    """this returns a db connection"""
    connection = psycopg2.connect(user="postgres",
                                  password="test",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="test")
    return connection


def init_remote_db_connection() -> Any:
    """I think we have to have the powershell instance going with the tunneling for this to work"""
    with open('db_remote_pass.txt') as f:
        read_data = f.read()
    connection = psycopg2.connect(user="howie",
                                  password=read_data,
                                  host="127.0.0.1",
                                  port="63333",
                                  database="howie")
    return connection




def addResponseToDB(connection, response, pg_table=''):
    kind = response.get('kind')
    pg_query = ""
    if kind == "youtube#video":
        pg_table = Table('videos')
    elif kind == "youtube#channel":
        pg_table = Table('channels')
    # elif kind == "youtube#searchResult":
    #    pg_table = ""  # Table('searches')
    else:
        print("No known kind: " + kind)
    if pg_table != '':
        if kind is not None:
            response.pop('kind')
        write_dict_to_db(connection, response, pg_table)



def update_used_status(conn, response: Dict):
    pg_table = Table(table_name_searched_insta_users)
    cursor = conn.cursor()
    # noinspection PyTypeChecker
    pg_query = Query.update(pg_table).set(pg_table.has_been_used, True).where(pg_table.username == response['username'])
    # pprint(pg_query)
    cursor.execute(pg_query.get_sql())
    conn.commit()
    cursor.close()


def update_api_key_status(conn, api_key):
    pg_table = Table(table_name_yt_api_keys)
    cursor = conn.cursor()
    # noinspection PyTypeChecker
    pg_query = Query.update(pg_table).set(pg_table.used_today, True).where(pg_table.api_key == api_key)
    # pprint(pg_query)
    cursor.execute(pg_query.get_sql())
    conn.commit()
    cursor.close()



def update_used_statuses(conn, responses: List[Dict]):
    for elem in responses:
        update_used_status(conn, elem)


def update_export_status(connection, responses, value=True, pg_table=''):
    cursor = connection.cursor()
    # exported_already, date_of_export
    for response in responses:
        channel_id = response.get('Channel Id')
        video_id = response.get('Video Id')
        if pg_table != '' and video_id is None and channel_id is None and pg_table != 'channels' and pg_table != 'videos':
            pg_table = Table(pg_table)
            query1 = Query.update(pg_table).set(pg_table.exported_already, value)
            # query2 = Query.update(pg_table).set(pg_table.date_of_export, str(datetime.today().strftime('%Y-%m-%d')))
            cursor.execute(query1.get_sql())
            # cursor.execute(query2.get_sql())
        if channel_id is not None and pg_table == '':
            pg_table = Table('channels')
            query1 = Query.update(pg_table).set(pg_table.exported_already, value)
            query2 = Query.update(pg_table).set(pg_table.date_of_export, str(datetime.today().strftime('%Y-%m-%d')))
            cursor.execute(query1.get_sql())
            cursor.execute(query2.get_sql())
            pg_table = ''
        if video_id is not None and pg_table == '':
            pg_table = Table('videos')
            query1 = Query.update(pg_table).set(pg_table.exported_already, value)
            query2 = Query.update(pg_table).set(pg_table.date_of_export, str(datetime.today().strftime('%Y-%m-%d')))
            cursor.execute(query1.get_sql())
            cursor.execute(query2.get_sql())
            pg_table = ''


def write_dict_to_db(connection, response, pg_table):
    # pprint(response)
    # response2 = add_exported_already_field(response, pg_table)
    # response3 = add_date_exported_field(response2, pg_table)
    cursor = connection.cursor()
    pg_query2 = PostgreSQLQuery.into(pg_table).insert(*response.values())
    pg_query = pg_query2.on_conflict().do_nothing()
    # pprint(pg_query2)
    cursor.execute(pg_query.get_sql())
    connection.commit()
    cursor.close()
    return cursor.rowcount


def write_dictlist_to_db(conn, dict_list: List[Dict], pg_table):
    rows_affected = 0
    for this_dict in dict_list:
        row_affected = write_dict_to_db(conn, this_dict, pg_table)
        rows_affected += 1
    print(f"Number of rows updated: {rows_affected}")


def flatten_list(list2d):
    """@safe"""
    merged = list(itertools.chain.from_iterable(list2d))
    return merged


def determine_table_from_filename(filepath: str) -> str:
    """@safe
    |  This can return an empty string if the filename doesn't correspond to a table."""
    if not is_basename(filepath):
        filepath = basename(filepath)
    if 'top' in filepath and 'instagram' in filepath:
        return 'instagram_post3'
    elif 'analysis' in filepath and 'instagram' in filepath:
        return 'instagram_user3'
    elif 'top' in filepath and 'youtube' in filepath:
        return 'videos'
    elif 'analysis' in filepath and 'youtube' in filepath:
        return 'channels'
    else:
        return ''


def is_basename(filepath):
    """@safe"""
    if '\\' in filepath or '/' in filepath:
        return False
    else:
        return True


def write_csv_to_db(csv_file_path, conn):
    dict_list = read_csv_into_dictlist(csv_file_path)
    table_name = determine_table_from_filename(csv_file_path)
    if table_name != '':
        write_dictlist_to_db(conn, dict_list, table_name)
    else:
        print("Could not write CSV to DB. Table name was empty.")


def get_toplevel_files_in_dir(dir_path):
    """@safe"""
    onlyfiles = [f for f in listdir(dir_path) if isfile(join(dir_path, f))]
    files = []
    for file in onlyfiles:
        full_path = join(dir_path, file)
        files.append(full_path)
    return files


def write_csvs_to_db(csv_folder_path, conn, has_been_exported_already=True):
    files = get_toplevel_files_in_dir(csv_folder_path)
    for file in files:
        write_csv_to_db(file, conn)



def get_category_ids_from_file(filepath='youtube_category_ids.txt'):
    with open(filepath) as f:
        mylist = f.read().splitlines()
    # pprint(mylist)
    return mylist


def add_exported_already_field(dict: Dict, table_name: str) -> Dict:
    """@safe Copies dict, then returns copy of that dict with added key."""
    dict2 = copy.deepcopy(dict)
    if table_name == 'instagram_post3' or table_name == 'instagram_user3':
        dict2['has_been_exported'] = True
    return dict2


def add_date_exported_field(dict: Dict, table_name: str) -> Dict:
    """@safe Copies dict, then returns copy of that dict with added key."""
    dict2 = copy.deepcopy(dict)
    if table_name == 'channels' or table_name == 'videos':
        dict2['date_of_export'] = datetime.today()
    return dict2


def main():
    pass


def copy_db_to_another(conn, db_name1, db_name2):
    db_table1 = load_db_table(conn, db_name1)
    # for instagram_users
    for elem in db_table1:
        username = elem['user_id']
        d = elem['username']
        a = elem['num_followers']
        w = elem['website']
        dd = elem['bio']
        ddd = elem['total_like_count']
        eee = elem['has_been_exported']
    write_dictlist_to_db(conn, db_table1, db_name2)


def remove_dupes_from_searched_users(conn):
    searched_users = load_db_table(conn, 'instagram_users_from_search')
    users = load_db_table(conn, 'instagram_users')
    usernames = get_dict_list_vals_for_key(users, 'user_id')
    searched_usernames = get_dict_list_vals_for_key(searched_users, 'username')
    dict_list = []
    for elem in searched_usernames:
        if elem not in usernames:
            print(elem)
            dict_list.append({'username': elem})
    write_dictlist_to_db(conn, dict_list, 'instagram_users_searched')


def get_list_of_searched_users_not_in_db_already(conn):
    searched_users = load_db_table(conn, table_name_searched_insta_users)
    users = load_db_table(conn, table_name_insta_users)
    usernames = get_dict_list_vals_for_key(users, 'user_id')
    searched_usernames = get_dict_list_vals_for_key(searched_users, 'username')
    dict_list = []
    dict_list_already_there = []
    for elem in searched_usernames:
        if elem not in usernames:
            dict_list.append({'username': elem})
        else:
            dict_list_already_there.append({'username': elem})

    return dict_list,dict_list_already_there



def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def get_channel_ids_not_in_channels(conn):
    channels_table = load_db_table_unexported(conn, table_name_channels)
    videos_table = load_db_table_unexported(conn, table_name_videos)
    chan_ids = get_dict_list_vals_for_key(channels_table, 'channel_id')
    vid_chan_ids = get_dict_list_vals_for_key(videos_table, 'channel_id')
    return list(set(vid_chan_ids) - set(chan_ids))


def get_random_word(conn, search_type, platform) -> str:
    clean_words = get_filtered_words()
    words_used = load_db_table(conn, table_name_words_used)
    words_not_to_use = []
    for elem in words_used:
        if elem['search_type'] == search_type and elem['platform'] == platform:
            words_not_to_use.append(elem['word'])
    x = random.choice(list(set(clean_words) - set(words_not_to_use)))
    return x


def get_random_country_code(conn, search_type, platform) -> str:
    clean_words = get_country_codes()
    words_used = load_db_table(conn, table_name_words_used)
    words_not_to_use = []
    for elem in words_used:
        if elem['search_type'] == search_type and elem['platform'] == platform:
            words_not_to_use.append(elem['word'])
    x = random.choice(list(set(clean_words) - set(words_not_to_use)))
    return x


def get_random_words(conn, search_type, platform, n) -> List[str]:
    clean_words = get_filtered_words()
    words_used = load_db_table(conn, table_name_words_used)
    words_not_to_use = []
    for elem in words_used:
        if elem['search_type'] == search_type and elem['platform'] == platform:
            words_not_to_use.append(elem['word'])

    x = random.sample(list(set(clean_words) - set(words_not_to_use)), n)
    return x


def main_test():
    conn = init_db_connection()
    x = get_random_country_code(conn, '', '')
    print(len(x))
    print(x)
    if conn:
        conn.close()


if __name__ == '__main__':
    conn = init_db_connection()
    x,y = get_list_of_searched_users_not_in_db_already(conn)
    print(len(x))
    print(len(y))
    if conn:
        conn.close()
    #main_test()
