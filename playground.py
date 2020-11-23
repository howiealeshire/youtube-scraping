import json
from dataclasses import asdict

from helper_functions import *
from enum import Enum

from table_data import YtAPIKey


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




def get_videos_and_channels_for_daily_export():
    conn = init_db_connection()
    vid_table = load_db_table_unexported(conn, 'videos')
    print(len(vid_table))
    channel_table = load_db_table_unexported(conn, 'channels')
    print(len(channel_table))
    write_dictlist_to_csv(channel_table, r'C:\Users\howie\PycharmProjects\pythonProject\data3\analysis_youtube_11_10_2020.csv')
    write_dictlist_to_csv(vid_table, r'C:\Users\howie\PycharmProjects\pythonProject\data3\top_youtube_11_10_2020.csv')

def main_test2():
    conn = init_db_connection()
    table = load_db_table(conn, 'videos')
    channel_ids = get_dict_list_vals_for_key(table,'channel_id')
    write_json_pprint(channel_ids,'channel_id_list.json')
    if conn:
        conn.close()

def main_test4():
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
    random_words = get_filtered_words()
    with open(' C:/Users/username/Desktop/ya_know_what_it_is.json', 'w') as outfile:
        json.dump(country_codes, outfile)
    #path = r'C:\Users\howie\PycharmProjects\pythonProject\test_searched_users.json'
    #get_n_users_from_db_and_write_to_json(init_db_connection(),path,5)


def test_test2():
    with open('db_remote_pass.txt') as f:
        read_data = f.read()
    pprint(read_data)


def test_test3():
    with open('youtube_api_keys.txt') as f:
        read_data = f.readlines()
    dict_list = []
    for elem in read_data:
        dict_list.append(asdict(YtAPIKey(elem,False)))
    write_dictlist_to_db(init_remote_db_connection(), dict_list, table_name_yt_api_keys)
    pprint(read_data)


if __name__ == "__main__":
    #print(len(read_csv_into_dictlist(r'C:\Users\howie\PycharmProjects\pythonProject\data3\analysis_youtube_11_10_2020.csv')))

    #get_videos_and_channels_for_daily_export()

    test_test3()
