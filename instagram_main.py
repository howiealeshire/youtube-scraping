import codecs
from os import listdir
from os.path import isfile, join

import requests
import ssl

from instagram_private_api import Client, ClientCompatPatch, ClientCookieExpiredError, ClientLoginRequiredError, \
    ClientLoginError, ClientError
import csv
import sys
import json

from pprint import pprint
from time import sleep
import os, uuid

import psycopg2
import psycopg2.extras
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

import xlsxwriter

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import itertools
from pypika import Query, Table, Field, PostgreSQLQuery
from datetime import datetime

from pypika.terms import Values
from flatten_dict import flatten
from random import randint

def append_dictlist_to_csv(dict_list, file_name):
    if (len(dict_list) > 0):
        with open(file_name, 'a', encoding='utf8', newline='') as output_file:
            fc = csv.DictWriter(output_file,
                                fieldnames=dict_list[0].keys(),

                                )
            fc.writeheader()
            fc.writerows(dict_list)
    else:
        print("No items to write. Aborting.")
        sys.exit()


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

def read_in_csv(file_path):
    with open(file_path, encoding='utf-8') as f:
        a = [{k: v for k, v in row.items()}
             for row in csv.DictReader(f, skipinitialspace=True)]
    return a


def get_top_users_by_hashtag(api, hashtag):
    filepath_to_write_to = "instagram_data/flattened_data.json"
    results = api.tag_section(hashtag)
    flattened_json = flatten_json(results)
    write_json_to_file(filepath_to_write_to, flattened_json)
    # parsed_data  = parse_tag_section_data(results)
    # write_objects_to_db(parsed_data)
    # write_objects_to_csv(parsed_data)
    # pass


def write_json_to_file(filepath, json_data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def read_json_from_file(filepath):
    with open('instagram_data/data.json', encoding='utf-8') as infile:
        return json.load(infile)


def write_objects_to_csv(objects):
    pass


def parse_tag_section_data(results):
    pass


def get_user_objects_from_section_data(results):
    pass


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


def write_objects_to_db(objects):
    pass


def main2():
    user_name = 'devdevdevdevdev123455667778989'
    password = 'hGC%\$;Q$J^2q3!]'
    api = Client(user_name, password, auto_patch=True)

    rank_token = api.generate_uuid()
    # results = api.user_followers(api.authenticated_user_id,rank_token)
    # pprint(results)
    # results = api.tag_search("#love",rank_token)
    # results = api.tag_section("love")
    results = api.user_info('2210641')
    pprint(results)
    # results = api.media_info("2402425765923413526")
    #
    """
    d = ""
    with open('data.json', encoding='utf-8') as infile:
        d = json.load(infile)
    d = flatten(d)
    pprint(d)
    """
    # results = api.explore()
    # results = flatten(results)
    # write_dictlist_to_csv(results,"instagram_parsed_results.csv")
    # pprint(results[0])
    # items = results.get('items', [])
    # pprint(items)


def to_json(python_object):
    if isinstance(python_object, bytes):
        return {'__class__': 'bytes',
                '__value__': codecs.encode(python_object, 'base64').decode()}
    raise TypeError(repr(python_object) + ' is not JSON serializable')


def from_json(json_object):
    if '__class__' in json_object and json_object['__class__'] == 'bytes':
        return codecs.decode(json_object['__value__'].encode(), 'base64')
    return json_object


def onlogin_callback(api, new_settings_file):
    cache_settings = api.settings
    with open(new_settings_file, 'w') as outfile:
        json.dump(cache_settings, outfile, default=to_json)
        print('SAVED: {0!s}'.format(new_settings_file))


def init_api(proxy=""):
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
                    on_login=lambda x: onlogin_callback(x, settings_file),
                    proxy=proxy)
            else:
                api = Client(
                    user_name, password,
                    on_login=lambda x: onlogin_callback(x, settings_file))
        else:
            with open(settings_file) as file_data:
                cached_settings = json.load(file_data, object_hook=from_json)
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
                on_login=lambda x: onlogin_callback(x, settings_file),
                proxy=proxy)
        else:
            api = Client(
                user_name, password,
                device_id=device_id,
                on_login=lambda x: onlogin_callback(x, settings_file))


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


def get_total_like_count_for_user(api, username):
    keep_going = True
    like_count = 0
    max_id = 0
    while keep_going:
        if (max_id == 0):
            try:
                user_info = api.username_feed(username)
            except Exception:
                print("Something went wrong with this user:")
                print(username)
                break
        else:
            try:
                user_info = api.username_feed(username, max_id=max_id)
            except Exception:
                print("Something went wrong with this user and max id:")
                print("Username")
                print(username)
                print("Max id")
                print(str(max_id))
                break
        max_id = user_info.get('next_max_id')
        if max_id is None:
            items = user_info.get('items')
            if items is not None:
                for item in items:
                    if item is not None:
                        return_json = turn_post_json_into_db_ready_object(item)
                        returned_like_count = return_json.get('like_count')
                        like_count += returned_like_count

            keep_going = False
            print("Finished last page")
            print("Final like count:")
            print(like_count)
            print("No more items")
            break
        else:
            items = user_info.get('items')
            for item in items:
                if item is not None:
                    return_json = turn_post_json_into_db_ready_object(item)
                    returned_like_count = return_json.get('like_count')
                    like_count += returned_like_count
            print("Finished page")
            print("Current like count:")
            print(like_count)
        print("Starting n e s t e d sleep")
        sleep(7)
        print("ending nested sleep")

    return like_count

def test_test_test():
    pass

def main():
    api = init_api()
    rank_token = api.generate_uuid()

    json_data = read_json_from_file("instagram_data/tag_section_data.json")
    posts = get_post_objects_from_section_data(json_data)
    parsed_post_list = []
    parsed_user_infos = []
    print("NUM USERS")
    print(len(posts))

    for post in posts:
        parsed_post = turn_post_json_into_db_ready_object(post)
        user_id = parsed_post.get('user_id')
        user_info = api.user_info(user_id)
        parsed_user_info = parse_user_info(user_info)
        username = parsed_user_info.get('username')
        if username is not None:
            parsed_user_info["total_like_count"] = get_total_like_count_for_user(api, username)

        parsed_post_list.append(parsed_post)
        parsed_user_infos.append(parsed_user_info)
        print("STARTING SLEEP")
        sleep(10)
        print("ENDING SLEEP")
    print("PARSED USER INFOS")
    pprint(parsed_user_infos)
    print("PARSED POST LIST")
    pprint(parsed_post_list)
    append_dictlist_to_csv(parsed_user_infos, "instagram_top_users.csv")
    append_dictlist_to_csv(parsed_post_list, "instagram_top_posts.csv")
    # x = api.media_info("2409076869281512599_4516619488")
    # pprint(x)
    # get_top_users_by_hashtag(api, "love")


def main_test():
    user_name = 'testestestestestestestestestde'
    password = 'hGC%\$;Q$J^2q3!]'
    # api = Client(user_name, password, auto_patch=True)
    api = init_api()
    rank_token = api.generate_uuid()
    # user_info = api.username_feed("devdevdevdevdev123455667778989")
    user_info = get_total_like_count_for_user(api, "devdevdevdevdev123455667778989")
    # write_json_to_file("instagram_data/user_info.json",user_info)
    pprint(user_info)
    # response = requests.get("https://api.instagram.com/v1/users/45951573/media/recent/?__a=1")
    # pprint(response)


def test_requests_main():
    url = "https://www.instagram.com/leomessi/?__a=1"
    proxy_host = "proxy.crawlera.com"
    proxy_port = "8010"
    with open('proxy_api_key.txt') as f:
        proxy_api_key = f.readline()

    proxy_auth = proxy_api_key + ":"  # Make sure to include ':' at the end
    proxies = {"https": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
               "http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}

    # api = init_api(proxies.get('https'))

    r = requests.get(url, proxies=proxies,
                     verify=False)

    print("""
    Requesting [{}]
    through proxy [{}]

    Request Headers:
    {}

    Response Time: {}
    Response Code: {}
    Response Headers:
    {}

    """.format(url, proxy_host, r.request.headers, r.elapsed.total_seconds(),
               r.status_code, r.headers, r.json()))


def parse_user_info(user_info):
    user = user_info.get('user')
    user_name = user.get('username')
    num_followers = user.get('follower_count')
    user_id = user.get('id')
    bio = user.get('biography')
    website = user.get('website')
    user_info_parsed = {
        'username': user_name,
        'num_followers': num_followers,
        'user_id': user_id,
        'bio': bio,
        'website': website
    }
    return user_info_parsed


def get_follower_counts_for_user(file_name):
    api = init_api()
    csv_dict_list = read_in_csv(join('C:/Users/howie/PycharmProjects/pythonProject/export_today_add_follower_counts',file_name))

    def create_user_set(csv_dict_list):
        userid_set = []
        for elem in csv_dict_list:
            username = elem.get('user_id')
            if username is not None and username != '' and username != 'user_id':
                userid_set.append(username)
        userid_set_true = set(userid_set)
        return userid_set_true

    userid_set = create_user_set(csv_dict_list)
    pprint(userid_set)

    def parse_user_infos(userid_set):
        parsed_user_dict = {}
       # parsed_results_list = []
        print("parsing info 1")
        for elem in userid_set:
            pprint(elem)
            results = api.user_info(elem)
            parsed_results = parse_user_info(results)
            parsed_user_dict[elem] = parsed_results
            print("parsed, now sleeping")
            sleep(randint(7,12))
            print("done sleeping, onto next")
        return parsed_user_dict
    parsed_results  = parse_user_infos(userid_set)
    print("parsed all user infos")
    for elem in csv_dict_list:
        id = elem.get('user_id')
        user_id_info = parsed_results.get(id)
        if user_id_info is not None:
            elem['num_followers'] = user_id_info['num_followers']
            elem['bio'] = user_id_info['bio']
            elem['website'] = user_id_info['website']
    print("writing to file")
    append_dictlist_to_csv(csv_dict_list, join('C:/Users/howie/PycharmProjects/pythonProject/export_today', file_name))
    print("done")



def build_follower_count_dict(dir_path,output_filename='follower_counts.json', input_file_path=''):
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

    mypath = dir_path
    if input_file_path != '':
        onlyfiles = [input_file_path]
    else:
        onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    csv_dict_list_list = []
    for elem in onlyfiles:
        if input_file_path == '':
            read_path = join(mypath, elem)
        else:
            read_path = input_file_path
        csv_dict_list = read_in_csv(read_path)
        pairs = get_id_follower_pairs(csv_dict_list)
        csv_dict_list_list.append(pairs)
    finalMap = {}
    for d in csv_dict_list_list:
        finalMap.update(d)
    json.dump(finalMap, open(output_filename,'w'))
    pprint(finalMap)
    return finalMap

def write_follower_counts_to_files(dir_path_input,dir_path_output):
    mypath = dir_path_input
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    pprint(onlyfiles)
    user_id_follower_count_dict = json.load(open('C:/Users/howie/PycharmProjects/pythonProject/follower_counts.json','r'))
    for elem in onlyfiles:
        read_path = join(dir_path_input,elem)
        csv_dict_list = read_in_csv(read_path)
        for item in csv_dict_list:
            user_id = item.get('user_id')
            retrieved_count = user_id_follower_count_dict.get(user_id)
            if retrieved_count is not None:
                item['followerCount'] = retrieved_count
        print("writing to file")
        write_dictlist_to_csv(csv_dict_list,
                               join(dir_path_output, elem))
        print("done")
        print("Edited dict:")
        #pprint(csv_dict_list)

    csv_dict_list_list = []



def get_analysis_insta_file_locations():
    mypath = "export_today"
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    top_post_files = []
    top_users_files = []
    #for elem in onlyfiles:
        #top_users_files.append(elem)
        #if "instagram" in elem:
        #    if "top" in elem:
        #        top_post_files.append(elem)
        #    else:
        #        top_users_files.append(elem)
    return onlyfiles

def get_all_users():
    mypath = "export_today_in_progress_test"
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    top_post_files = []
    top_users_files = []
    api = init_api()
    user_list = []

    def get_users(csv_dict_list):
        filtered_list = []
        for elem in csv_dict_list:
            if elem.get('username'):
                filtered_list.append(elem.get('username'))
        return filtered_list

    list2d = [[1, 2, 3], [4, 5, 6], [7], [8, 9]]
    for file_name in onlyfiles:
        csv_dict_list = read_in_csv(join(mypath, file_name))
        users = get_users(csv_dict_list)
        user_list.append(users)
    #merged = list(itertools.chain(*user_list))
    return user_list #list(set(merged))


def get_array_and_delete_from_file(filename):
    f = open(filename,'r')
    lst = json.load(f)
    arr_arg = lst[0]
    print(len(lst))
    lst.pop(0)
    f.close()
    print(len(lst))
    f = open(filename,'w')
    json.dump(lst, f)
    f.close()
    return arr_arg


if __name__ == "__main__":
    #main2()
    #get_follower_counts_for_user()\
    #x = get_all_users()
    #pprint(x)
    #print(len(x))
    # Read in the file

    path = r'C:\Users\howie\PycharmProjects\pythonProject\instascraper\args.json'
    path_copy =  r'C:\Users\howie\PycharmProjects\pythonProject\instascraper\args_copy.json'
    test_file =  r'C:\Users\howie\PycharmProjects\pythonProject\instascraper\test_args.json'
    replacement_data = r'C:\Users\howie\PycharmProjects\pythonProject\instascraper\args.txt'
    """
    with open(replacement_data, 'r') as file:
        filedata = file.read()

    # Replace the target string
    filedata = filedata.replace('\'', '\"')

    # Write the file out again
    with open(path_copy, 'w') as file:
        file.write(filedata)
    with open(path, 'w') as file:
        file.write(filedata)
    """
   # user_list = get_all_users()
   # pprint(user_list)
   # json.dump(user_list,open(test_file, 'w'))
    #pprint(get_array_and_delete_from_file(test_file))
    #get_follower_counts_for_user()

    #get_array_and_delete_from_file(open(r'C:\Users\howie\PycharmProjects\pythonProject\instascraper\args_copy.txt'))
    #pprint(get_analysis_insta_file_locations())
    #get_follower_counts_for_user('analysis_instagram_2020-10-27.csv')
    #get_follower_counts_for_user('analysis_instagram_2020-10-28.csv')
    build_follower_count_dict()

    #test_dict = read_in_csv(r'C:\Users\howie\PycharmProjects\pythonProject\add_already_existing_follower_counts2\analysis_instagram_2020-10-12.csv')
    #pprint(test_dict)
    #main2()
    #write_follower_counts_to_files()
    #for file in files:
    #    print("Started file: " + str(file))
    #    get_follower_counts_for_user(file)
    #    print("Finished file: " + str(file))
    #    sleep(randint(7, 12))
# Note: instagram post url is constructed as follows:
#  https://www.instagram.com/p/CFfLH3Jnff6/ where CFfLH3Jnff6 is the code field of the media object
