import json

from helper_functions import *
from enum import Enum

from instagram_main import build_follower_count_dict, write_follower_counts_to_files
from youtube_data_flows import get_channels_from_channel_ids


class TableNames(Enum):
    instagram_posts = 'instagram_post3',
    instagram_users = 'instagram_user3',
    instagram_user_from_search = 'instagram_users_from_search',
    youtube_channels = 'channels',
    youtube_videos = 'videos'


def main():
    with init_db_connection() as conn:
        searched_users_table = load_db_table(conn, table_name='instagram_users_from_search')
        insta_users_table = load_db_table(conn, table_name='instagram_user3')
        searched_users: List[Any] = get_dict_list_vals_for_key(searched_users_table, 'username')
        insta_users: List[Any] = get_dict_list_vals_for_key(insta_users_table, 'username')
        list_of_non_dupes = []
        for searched_user in searched_users:
            if searched_user not in insta_users:
                list_of_non_dupes.append(searched_user)
    with open('instagram_users_to_scrape_today.json', 'w', encoding='utf-8') as f:
        json.dump(list_of_non_dupes, f)
    return list_of_non_dupes


def main_test():
    path = 'country_codes.json'
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    items = data.get('items')
    country_codes = []
    for item in items:
        country_code = item.get('id')
        if country_code:
            country_codes.append(country_code)
    pprint(country_codes)


def main_test2():
    conn = init_db_connection()
    table = load_db_table(conn, 'videos')
    channel_ids = get_dict_list_vals_for_key(table,'channel_id')
    write_json_pprint(channel_ids,'channel_id_list.json')
    if conn:
        conn.close()


def main_test3():
    #build_follower_count_dict('','follower_counts.json',r'C:\Users\howie\PycharmProjects\pythonProject\add_already_existing_follower_counts5\test_11_09_2020.csv')
    write_follower_counts_to_files('add_already_existing_follower_counts5','add_already_existing_follower_counts7')
if __name__ == "__main__":
    main_test3()
