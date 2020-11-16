import itertools
import json
import sys
from dataclasses import asdict
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

from helper_functions import read_csv_into_dictlist, append_dictlist_to_csv, write_dictlist_to_csv, init_db_connection, \
    update_export_status, write_dict_to_db, write_dictlist_to_db, update_used_statuses
from table_data import InstaUser, InstaPost, table_name_insta_users, table_name_posts


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


def make_top_posts_list(csv_rows) -> List[Dict]:
    top_users_dict_list = []
    for row in csv_rows:
        top_post = InstaPost(comment_count=int(row.get('commentCount')),
                             like_count=int(row.get('likeCount')),
                             user_id=int(row.get('user_id')),
                             username=row.get('username'),
                             postURL=row.get('postURL'),
                             total_like_count=int(row.get('total_like_count')),
                             caption=row.get('captions'),
                             video_url=row.get('videoURL'),
                             image_url=row.get('image_url'))
        top_users_dict_list.append(asdict(top_post))
    return top_users_dict_list

def make_top_users_list(csv_rows) -> List[Dict]:
    top_users_dict_list = []
    for row in csv_rows:
        top_user = InstaUser(user_id=int(row.get('user_id')),
                             username=row.get('username'),
                             num_followers=int(row.get('followerCount')),
                             website=row.get('website'),
                             bio=row.get('biography'),
                             total_like_count=int(row.get('total_like_count')))
        top_users_dict_list.append(asdict(top_user))
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
        if like_count == 'likeCount':
            continue
        id_like_count_dict[user_id] += int(like_count)
    for row in csv_dict_list:
        user_id = row.get('user_id')
        like_count = row.get('likeCount')
        total_like_count = id_like_count_dict[user_id]
        row['total_like_count'] = total_like_count
    return csv_dict_list


def build_follower_count_dict_and_return(csv_dict_list):
    def get_id_follower_pairs(csv_dict_list):
        id_bio_pairs_dict = {}
        for row in csv_dict_list:
            user_id = row.get('user_id')
            bio = row.get('followerCount')
            if bio is None:
                bio = row.get('num_followers')
            if user_id not in id_bio_pairs_dict.keys() and bio is not None:
                id_bio_pairs_dict[user_id] = bio
        return id_bio_pairs_dict

    pairs = get_id_follower_pairs(csv_dict_list)

    for elem in csv_dict_list:
        elem['followerCount'] = pairs[elem['user_id']]

    return csv_dict_list


def remove_rows_containing_zero_in_important_columns(csv_dict_list):
    csv_dict_list2 = []
    for elem in csv_dict_list:
        follower_count = elem['followerCount']
        total_like_count = elem['total_like_count']
        if not follower_count:
            follower_count = '0'
        if not total_like_count:
            total_like_count = '0'

        if int(follower_count) < 30:
            continue
        if int(total_like_count) < 30:
            continue
        else:
            csv_dict_list2.append(elem)
    return csv_dict_list2

def remove_header_line_after_header(csv_dict_list):
    csv_dict_list2 = []
    i = 0
    for elem in csv_dict_list:
        if elem.get('followerCount') == 'followerCount' and i != 0:
            continue
        else:
            csv_dict_list2.append(elem)
        i += 1
    return csv_dict_list2

def main2(path):
    csv_dict_list = read_csv_into_dictlist(path)
    csv_dict_list = remove_header_line_after_header(csv_dict_list)
    csv_dict_list = add_bio_to_each(csv_dict_list)
    csv_dict_list = make_websites_non_empty(csv_dict_list)
    csv_dict_list = add_total_likes_to_csv_dict_list(csv_dict_list)
    csv_dict_list = build_follower_count_dict_and_return(csv_dict_list)
    csv_dict_list = remove_rows_containing_zero_in_important_columns(csv_dict_list)
    top_users = make_top_users_list(csv_dict_list)
    top_posts = make_top_posts_list(csv_dict_list)

    return top_users, top_posts
    # write_dictlist_to_csv(top_users, "data3/analysis_" + filename)
    # write_dictlist_to_csv(top_posts, "data3/top_" + filename)


def main():
    main2(r'C:\Users\howie\PycharmProjects\pythonProject\add_already_existing_follower_counts5\test_11_10_2020.csv',
          'test_11_10_2020.csv')



if __name__ == "__main__":
    db_conn = init_db_connection()
    filepath = r'C:\Users\howie\PycharmProjects\pythonProject\instascraper\scrapy_exports\0.csv'

