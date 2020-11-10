import copy
import csv
import itertools
import json
import sys
from datetime import datetime
from os import listdir
from os.path import basename, isfile, join
from pprint import pprint
from typing import Dict, List, Any

import psycopg2
import psycopg2.extras
from pypika import Table, Query, PostgreSQLQuery
import os

from hypothesis import given
from hypothesis.strategies import text


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

    # cursor = conn.cursor()
    # cursor.execute(q.get_sql())
    # rows = cursor.fetchall()
    return ans1


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


def update_export_status(connection, responses, value='TRUE', pg_table=''):
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


def write_dict_to_db(connection, response: Dict, pg_table):
    response2 = add_exported_already_field(response, pg_table)
    response3 = add_date_exported_field(response2, pg_table)
    cursor = connection.cursor()
    pg_query2 = PostgreSQLQuery.into(pg_table).insert(*response3.values())
    pg_query = pg_query2.on_conflict().do_nothing()
    cursor.execute(pg_query.get_sql())
    connection.commit()
    cursor.close()


def write_dictlist_to_db(conn, dict_list: List[Dict], pg_table):
    for this_dict in dict_list:
        write_dict_to_db(conn, this_dict, pg_table)


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


def main_test():
    conn = init_db_connection()
    write_csvs_to_db(r'C:\Users\howie\PycharmProjects\pythonProject\test_folder', conn)


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)

if __name__ == '__main__':
    x = grouper([1,2,3,4,5,6,7,8,9,10],4,'')
    pprint(list(x))
