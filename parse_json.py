import itertools
import json
import sys
from datetime import datetime
from pprint import pprint
import csv
from typing import Dict, Any, List

from attr import dataclass
from mypy import *
import psycopg2
import psycopg2.extras
from random import randrange
from inspect import FrameInfo, unwrap

from requests import Response

from pypika import Query, Table, Field, PostgreSQLQuery
from datetime import datetime
from os import listdir
from os.path import isfile, join
import subprocess
import requests
import urllib.request
from better_profanity import profanity


from helper_functions import read_csv_into_dictlist, append_dictlist_to_csv, write_dictlist_to_csv, init_db_connection, update_export_status, addDictToDB
from instagram_dataflows import request_users_from_keyword_search, get_users_from_json, get_filtered_words


def parse(json_obj):
    data = json_obj
    # all that we have to do here is to parse the JSON we have
    user_id = data['entry_data']['ProfilePage'][0]['graphql']['user']['id']

    next_page_bool = \
        data['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']['page_info'][
            'has_next_page']
    edges = data['entry_data']['ProfilePage'][0]['graphql']['user']['edge_felix_video_timeline']['edges']
    item_list = []
    for i in edges:
        url = 'https://www.instagram.com/p/' + i['node']['shortcode']
        video = i['node']['is_video']
        date_posted_timestamp = i['node']['taken_at_timestamp']
        date_posted_human = datetime.fromtimestamp(date_posted_timestamp).strftime("%d/%m/%Y %H:%M:%S")
        like_count = i['node']['edge_liked_by']['count']
        if like_count is None:
            like_count = ''

        comment_count = i['node']['edge_media_to_comment']['count'] if 'edge_media_to_comment' in i[
            'node'].keys() else ''
        captions = ""
        if i['node']['edge_media_to_caption']:
            for i2 in i['node']['edge_media_to_caption']['edges']:
                captions += i2['node']['text'] + "\n"

        if video:
            image_url = i['node']['display_url']
        else:
            image_url = i['node']['thumbnail_resources'][-1]['src']
        item = {'postURL': url, 'isVideo': video, 'date_posted': date_posted_human,
                'timestamp': date_posted_timestamp, 'likeCount': like_count, 'commentCount': comment_count,
                'image_url': image_url,
                'captions': captions[:-1]}
        item_list.append(item)
    if next_page_bool:
        cursor = \
            data['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']['page_info'][
                'end_cursor']
        di = {'id': user_id, 'first': 12, 'after': cursor}
        print(di)
        params = {'query_hash': 'e769aa130647d2354c40ea6a439bfc08', 'variables': json.dumps(di)}
    return item_list

def add_bio_to_each(csv_dict_list):
    def get_id_bio_pairs(csv_dict_list):
        id_bio_pairs_dict = {}
        for row in csv_dict_list:
            user_id = row.get('user_id')
            bio = row.get('biography')
            if user_id not in id_bio_pairs_dict.keys() and bio is not None and bio != '':
                id_bio_pairs_dict[user_id] = bio
        return id_bio_pairs_dict

    id_bio_dict = get_id_bio_pairs(csv_dict_list)
    for row in csv_dict_list:
        if row.get('biography') == '':
            user_id = row.get('user_id')
            if user_id is not None:
                obtained_bio = id_bio_dict.get(user_id)
                if obtained_bio:
                    row['biography'] = obtained_bio
                else:
                    row['biography'] = ''

    return csv_dict_list


def make_top10_posts_list(csv_rows):
    top_users_dict_list = []
    user_id_set = set()
    for row in csv_rows:
        top_user_dict = {
            'comment_count': row.get('commentCount'),
            'like_count': row.get('likeCount'),
            'user_id': row.get('user_id'),
            'username': row.get('username'),
            'postURL': row.get('postURL'),
            'caption': row.get('captions'),
            'image_url': row.get('image_url'),
            'video_url': row.get('videoURL'),
            'total_like_count': row.get('total_like_count')
        }
        curr_user_id = top_user_dict.get('user_id')
        if (curr_user_id is not None and curr_user_id not in user_id_set):
            user_id_set.add(curr_user_id)
            top_users_dict_list.append(top_user_dict)
    non_zero_list = []
    for elem in top_users_dict_list:
        like_count = elem.get('like_count')
        if like_count != '0':
            non_zero_list.append(elem)
        else:
            # TEMPORARY
            non_zero_list.append(elem)
    return non_zero_list





def make_top_users_list(csv_rows):
    top_users_dict_list = []
    user_id_set = set()
    for row in csv_rows:
        top_user_dict = {
            'username': row.get('username'),
            'num_followers': row.get('followerCount'),
            'user_id': row.get('user_id'),
            'bio': row.get('biography'),
            'website': row.get('website'),
            'total_like_count': row.get('total_like_count'),
        }
        if (top_user_dict.get('user_id') not in user_id_set):
            user_id_set.add(top_user_dict.get('user_id'))
            top_users_dict_list.append(top_user_dict)
    return top_users_dict_list

def make_websites_non_empty(csv_dict_list):
    def get_id_bio_pairs(csv_dict_list):
        id_bio_pairs_dict = {}
        for row in csv_dict_list:
            user_id = row.get('user_id')
            bio = row.get('website')
            if user_id not in id_bio_pairs_dict.keys():
                id_bio_pairs_dict[user_id] = bio
        return id_bio_pairs_dict

    id_bio_dict = get_id_bio_pairs(csv_dict_list)
    for row in csv_dict_list:
        if row.get('website') == '':
            user_id = row.get('user_id')
            if user_id is not None:
                x = id_bio_dict[user_id]
                if x:
                    row['website'] = x
                else:
                    row['website'] = ''

    return csv_dict_list


def add_total_likes_to_csv_dict_list(csv_dict_list):
    def get_id_bio_pairs(csv_dict_list):
        id_bio_pairs_dict = {}
        for row in csv_dict_list:
            user_id = row.get('user_id')
            id_bio_pairs_dict[user_id] = 0
        return id_bio_pairs_dict

    id_like_count_dict = get_id_bio_pairs(csv_dict_list)
    for elem in csv_dict_list:
        user_id = elem.get('user_id')
        like_count = elem.get('likeCount')
        id_like_count_dict[user_id] += int(like_count)
    for row in csv_dict_list:
        user_id = row.get('user_id')
        like_count = row.get('likeCount')
        total_like_count = id_like_count_dict[user_id]
        row['total_like_count'] = total_like_count
    return csv_dict_list




def readDictListsAndExportToDB(dict_list_list, connection, cursor):
    pass


def main2(path, filename):
    # connection = psycopg2.connect(user="postgres",
    #                              password="test",
    #                              host="127.0.0.1",
    #                              port="5432",
    #                              database="postgres")
    # cursor = connection.cursor()
    # ddDictListToDB(cursor)
    test_file_path = path
    # test_file_path = "C:/Users/howie/PycharmProjects/pythonProject/instascraper/test13.csv"
    csv_dict_list = read_csv_into_dictlist(test_file_path)
    pprint(len(csv_dict_list))
    #could make add_bio_to_each and make_websites_non_empty into single function
    # : make_rows_into_canon_form
    csv_dict_list = add_bio_to_each(csv_dict_list)
    csv_dict_list = make_websites_non_empty(csv_dict_list)
    csv_dict_list = add_total_likes_to_csv_dict_list(csv_dict_list)
    top_users = make_top_users_list(csv_dict_list)
    top_posts = make_top10_posts_list(csv_dict_list)

    write_dictlist_to_csv(top_users, "add_already_existing_follower_counts2/analysis_" + filename)
    write_dictlist_to_csv(top_posts, "add_already_existing_follower_counts2/top_" + filename)
    # pprint(csv_dict_list)


def read_top_users_list(file_path):
    with open(file_path) as f:
        lines = f.read().splitlines()
    final_list = []
    for line in lines:
        line2 = line.strip()
        final_list.append(line2[1:])
    return final_list


def file_len(fname):
    with open(fname, encoding='utf-8') as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def main_test():
    connection = psycopg2.connect(user="postgres",
                                  password="test",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")
    mypath = "data"
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    top_post_files = []
    top_users_files = []
    for elem in onlyfiles:
        if "instagram" in elem:
            if "top" in elem:
                top_post_files.append(elem)
            else:
                top_users_files.append(elem)

    # pprint(top_post_files)
    # pprint(top_users_files)
    top_post_dict_list = []
    top_users_dict_list = []
    for elem in top_post_files:
        path = join('data', elem)
        dict_list = read_csv_into_dictlist(path)
        top_post_dict_list.append(dict_list)
    for elem in top_users_files:
        path = join('data', elem)
        dict_list = read_csv_into_dictlist(path)
        top_users_dict_list.append(dict_list)
    pprint(top_users_dict_list[:5])
    print("****")
    pprint(top_post_dict_list[:5])

    for elem in top_post_dict_list:
        for item in elem:
            addDictToDB(connection, item, 'instagram_post3')
    for elem in top_users_dict_list:
        for item in elem:
            addDictToDB(connection, item, 'instagram_user3')

    connection.close()
    # path = "C:/Users/howie/PycharmProjects/pythonProject/instascraper/test8.csv"
    # dict_list  = read_csv_into_dictlist(path)


def get_follower_counts_from_file_temp(filepath):
    pass


def automate():
    subprocess.run(""
                   "scrapy crawl instagram -o analysis_instagram_2020-10-27.csv"
                   "&& "
                   "scrapy crawl instagram -o analysis_instagram_2020-10-28.csv"
                   ""
                   , cwd="C:\\Users\\howie\\PycharmProjects\\pythonProject\\instascraper", shell=True)


def write_follower_counts_to_files(filepath, og_csv_dict_list):
    mypath = 'C:/Users/howie/PycharmProjects/pythonProject/add_already_existing_follower_counts2'

    user_id_follower_count_dict = json.load(
        open('C:/Users/howie/PycharmProjects/pythonProject/follower_counts.json', 'r'))
    # read_path = join('C:/Users/howie/PycharmProjects/pythonProject/export_today',elem)
    # csv_dict_list = read_in_csv(read_path)
    for item in og_csv_dict_list:
        user_id = item.get('user_id')
        retrieved_count = user_id_follower_count_dict.get(user_id)
        if retrieved_count is not None:
            item['followerCount'] = retrieved_count
    print("writing to file")
    # append_dictlist_to_csv(og_csv_dict_list,
    #                       join(mypath, elem))

    print("done")
    print("Edited dict:")
    # pprint(csv_dict_list)

    return og_csv_dict_list


def get_users_from_json_search_download(filepath='instagram_users_download'):
    search_result = json.load(open(filepath, 'r'))
    pre_parsed_users = search_result.get('users')
    usernames = []
    for elem in pre_parsed_users:
        user_data = elem.get('user')
        username = user_data.get('username')
        usernames.append(username)
    pprint(usernames)
    return usernames


# this one is literally the above function [get_users_from_json_search_download(filepath)], except i get it directly
# from the download, rather than creating the intermediate file


def filter_out_dupes():
    pass



def get_n_users_flow():
    filtered_words = get_filtered_words()
    filtered_word = filtered_words[randrange(len(filtered_words))]
    json_users = request_users_from_keyword_search(filtered_word)
    users = get_users_from_json(json_users)
    pprint(users)
    #get n users from json download
    #check for dupes in db







def print_trace(result):
    if hasattr(result,'_trace'):
        if result.trace is not None:
            for trace_line in result.trace:
                print(f"{trace_line.filename}:{trace_line.lineno} in `{trace_line.function}`")




if __name__ == "__main__":
    #main2()

    main2(r'C:\Users\howie\PycharmProjects\pythonProject\add_already_existing_follower_counts2\test1132020.csv','test1132020.csv')


