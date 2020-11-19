import os
import random
from dataclasses import asdict
from functools import wraps
from os import listdir
from os.path import isfile, join
import subprocess
from pprint import pprint
from threading import Timer
from time import sleep
from typing import Dict, List, Any, TypeVar

import requests
import urllib.request

from attr import dataclass
from better_profanity import profanity
from cytoolz.functoolz import pipe
from requests import Response
from helper_functions import init_db_connection, write_dictlist_to_db, get_random_words, update_used_statuses
from helper_functions import flatten_list
from parse_scrapy_insta_csv import main2
from table_data import table_name_posts, table_name_insta_users, SearchWord, table_name_words_used, InstaUserSearched, \
    table_name_searched_insta_users


def delayed(f):
    def new_f(*args, **kwargs):
        sleep(random.randint(7, 12))
        return f(*args, **kwargs)

    return new_f


def get_users_from_json(search_result: Dict[str, Any]) -> List[str]:
    """@safe"""
    pre_parsed_users = search_result.get('users')
    if pre_parsed_users is not None:
        usernames = []
        for elem in pre_parsed_users:
            username = elem.get('user').get('username')
            if username is not None:
                usernames.append(username)
        return usernames


def request_users_from_keyword_search(keyword) -> Response:
    """@impure_safe: Returns 100 results back."""
    return requests.get("https://www.instagram.com/web/search/topsearch/?context=blended&query=" + keyword)


def _fetch_user_profiles(keyword: str) -> List[str]:
    return pipe(
        keyword,
        request_users_from_keyword_search,
        _get_json_from_response,
        get_users_from_json)


def _get_json_from_response(response: requests.Response) -> Dict[str, str]:
    """@safe"""
    return response.json()






def get_n_sets_of_profiles(conn, n: int) -> List[str]:
    """The number of profiles returned is 100 times n."""
    search_type = 'get_n_sets_of_profiles'
    platform = 'instagram'
    chosen_words = get_random_words(conn,search_type,platform,n)
    user_profiles_list = []
    for word in chosen_words:
        user_profiles_list.append(_fetch_user_profiles(word))
    used_words = []
    for word in chosen_words:
        used_words.append(asdict(SearchWord(word,search_type,platform,True)))
    write_dictlist_to_db(conn,used_words,table_name_words_used)
    profile_list = []
    user_profiles_list2 = flatten_list(user_profiles_list)
    for profile in user_profiles_list2:
        profile_list.append(asdict(InstaUserSearched(profile,False)))
    write_dictlist_to_db(conn,profile_list,table_name_searched_insta_users)
    return flatten_list(user_profiles_list)




def get_top_users_by_hashtag(api, hashtag):
    filepath_to_write_to = "instagram_data/flattened_data.json"
    results = api.tag_section(hashtag)

    # flattened_json = flatten_json(results)
    # write_json_to_file(filepath_to_write_to, flattened_json)


def get_post_objects_from_section_data(results):
    sections = results.get('sections')  # section is array
    section_list = []
    for section in sections:
        section_list.append(section)
    media_list = []
    for section in section_list:
        layout_content = section.get('layout_content')
        medias = layout_content.get('medias')
        if medias is not None:
            for elem in medias:
                media = elem.get('media')
                media_list.append(media)

    return media_list


# post_json should be in the form of "media" key objects
def turn_post_json_into_db_ready_object(post_json):
    comment_count = post_json.get('comment_count')
    like_count = post_json.get('like_count')
    user = post_json.get('user')
    user_id = user.get('id')
    username = user.get('username')

    code = post_json.get('code')
    caption = post_json.get('caption')
    if caption is not None:
        caption_text = caption.get('text')
    else:
        caption_text = ""
    post_dict = {
        'comment_count': comment_count,
        'like_count': like_count,
        'user_id': user_id,
        'username': username,
        'code': code,
        'caption': caption_text
    }
    return post_dict


def run_scrapy(filenum: int):
    scrapy_filename = str(filenum) + ".csv"
    scrapy_path = 'instascraper/scrapy_exports' + scrapy_filename
    scrapy_short_path = os.path.join('scrapy_exports',scrapy_filename)
    print("starting scrape")
    subprocess.run("scrapy crawl instagram -o " + scrapy_short_path, cwd="instascraper"
                                                                   , shell=True)

    print("finished scrape")

    top_users, top_posts = main2(scrapy_path)

    return top_users, top_posts, scrapy_path

def run_scrapy_without_scrapy(db_conn, filenum: int):
    scrapy_filename = str(filenum) + ".csv"
    scrapy_path = 'instascraper/scrapy_exports' + scrapy_filename
    scrapy_short_path = os.path.join('scrapy_exports', scrapy_filename)
    print("starting scrape")
    #subprocess.run("scrapy crawl instagram -o " + scrapy_short_path, cwd="C:\\Users\\howie"
    #                                                                     "\\PycharmProjects"
    #                                                                     "\\pythonProject\\instascraper"
    #                                                                     "", shell=True)

    print("finished scrape")

    top_users, top_posts = main2(scrapy_path)

    write_dictlist_to_db(db_conn, top_users, table_name_insta_users)
    update_used_statuses(db_conn, top_users)
    write_dictlist_to_db(db_conn, top_posts, table_name_posts)
    update_used_statuses(db_conn, top_posts)
    #os.remove(scrapy_path)

def main():
    scrapy_path = r'C:\Users\howie\PycharmProjects\pythonProject\instascraper\scrapy_exports\test_file.csv'
    top_users, top_posts = main2(scrapy_path)
    write_dictlist_to_db(init_db_connection(), top_posts, table_name_posts)
    write_dictlist_to_db(init_db_connection(), top_users, table_name_insta_users)

def main_test():
    conn = init_db_connection()
    #x = get_random_word(conn,'','')
    #print(x)
    run_scrapy_without_scrapy(conn,2)
    if conn:
        conn.close()

if __name__ == '__main__':
    main_test()
