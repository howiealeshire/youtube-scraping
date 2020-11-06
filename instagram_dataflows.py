import random
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
from helper_functions import read_csv_into_dictlist, append_dictlist_to_csv, write_dictlist_to_csv, init_db_connection, update_export_status, write_dict_to_db
from helper_functions import flatten_list

def delayed(f):
    def new_f(*args,**kwargs):
        sleep(random.randint(7, 12))
        return f(*args, **kwargs)
    return new_f


def get_users_from_json(search_result: Dict[str,Any]) -> List[str]:
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

def get_filtered_words():
    with open('clean_words.txt') as f:
        return f.readlines()

def get_n_sets_of_profiles(n: int) -> List[str]:
    """The number of profiles returned is 100 times n."""
    filtered_words = get_filtered_words()
    chosen_words = random.sample(filtered_words, n)
    user_profiles_list = []
    for word in chosen_words:
        user_profiles_list.append(_fetch_user_profiles(word))
    return flatten_list(user_profiles_list)



def main():
    pass

if __name__ == '__main__':
    pprint(get_n_sets_of_profiles(2))