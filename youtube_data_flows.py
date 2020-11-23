import itertools
import random
from dataclasses import asdict
from pprint import pprint
from typing import List, Any, Dict

from helper_functions import init_db_connection, write_dictlist_to_db, flatten_list, get_dict_list_vals_for_key, \
    write_dict_to_db, get_random_word, get_random_country_code
from table_data import table_name_videos, table_name_channels, SearchWord, table_name_words_used, Region, \
    table_name_country_codes
from yt_main import init_yt_client, makeSearchRequestsForNRecordsClean, flattenAndParseChannelResponse, \
    flattenAndParseResponses

num_pages_of_results = 8
max_results_per_page = 50


def flatten_and_parse_all_responses(response_array) -> List[List[dict]]:
    """@safe | json written to <function_name>.json. It is the result of calling this function after
       get_all_items_from_response with sample response listed at that function.
        """
    parsed_flattened_arr = []
    for elem in response_array:
        parsed_response2 = flattenAndParseResponses(elem)
        parsed_flattened_arr.append(parsed_response2)

    true_flattened = parsed_flattened_arr
    return true_flattened


def get_all_items_from_response(response_array) -> List[Any]:
    """@safe | json written to <function_name>.json. It is the result of calling this function after
    makeSearchRequestsForNRecordsClean with sample response listed at that function.
    """
    merged = []
    for elem in response_array:
        items = elem.get('items')
        if items is not None:
            merged.append(items)
    merged = list(itertools.chain.from_iterable(merged))
    return merged


def get_most_popular_vids(conn):
    search_type = 'get_most_popular_vids'
    platform = 'youtube'
    regionCode = get_random_country_code(conn,search_type,platform)
    used_regions = []
    used_regions.append(asdict(Region(regionCode, search_type, platform, True)))
    youtube = init_yt_client(conn)
    search_params_most_popular = dict(maxResults=max_results_per_page, part='snippet,contentDetails,statistics',
                                      chart="mostPopular", pageToken="CDIQAA",
                                      regionCode=regionCode)

    request_array2, response_array2 = makeSearchRequestsForNRecordsClean(youtube, num_pages_of_results, search_params_most_popular, True)
    merged = get_all_items_from_response(response_array2)
    parsed_flattened_arr = flatten_and_parse_all_responses(merged)
    write_dictlist_to_db(conn,used_regions,table_name_country_codes)
    return parsed_flattened_arr


def get_most_popular_channels(conn):
    search_type = 'get_most_popular_channels'
    platform = 'youtube'
    youtube = init_yt_client(conn)
    chosen_word = get_random_word(conn,search_type,platform)
    write_dict_to_db(conn,asdict(SearchWord(chosen_word,search_type,platform,True)),table_name_words_used)

    search_params = dict(maxResults=max_results_per_page, order='viewCount', part='snippet', type='channel',
                         pageToken="CDIQAA", q=chosen_word)
    request_array2, response_array2 = makeSearchRequestsForNRecordsClean(youtube,num_pages_of_results, search_params, False, False)
    merged = get_all_items_from_response(response_array2)
    parsed_flattened_arr = flatten_and_parse_all_responses(merged)

    channel_ids = get_dict_list_vals_for_key(parsed_flattened_arr, 'channel_id')
    channel_results = get_channel_from_channel_id(channel_ids)
    return channel_results


def flatten_and_parse_all_responses_channels_test(youtube, response_array, video=False):
    """@safe | json written to <function_name>.json. It is the result of calling this function after
       get_all_items_from_response with sample response listed at that function.
        """

    parsed_flattened_arr = []
    for elem in response_array:
        channel_id = elem.get('id').get('channelId')
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()
        item = response.get('items')
        if item:
            item = item[0]
        parsed_response2 = flattenAndParseResponses(item)
        parsed_flattened_arr.append(parsed_response2)
    return parsed_flattened_arr


def get_channel_from_channel_id(conn, channel_ids_list: List[str]):
    youtube = init_yt_client(conn)
    search_params = []
    dict_list_list = []
    for elem in channel_ids_list:
        dict_list, new_api = get_channel_from_channel_id_true(conn, youtube, elem)
        dict_list_list.append(dict_list)
        if new_api:
            youtube = new_api

    return flatten_list(dict_list_list)

def get_channel_from_channel_id_true(conn,youtube, channel_id: str):
    search_params = dict(maxResults=max_results_per_page, part="snippet,contentDetails,statistics",
                              pageToken="CDIQAA", id=channel_id)
    had_to_replace_api = False
    request_array, response_array, youtube2 = makeSearchRequestsForNRecordsClean(youtube, num_pages_of_results, search_params, False, True)
    if not youtube2:
        youtube = init_yt_client(conn)
        request_array, response_array, youtube2 = makeSearchRequestsForNRecordsClean(youtube, num_pages_of_results,
                                                                                     search_params, False, True)

    merged = get_all_items_from_response(response_array)
    dict_list = []
    for elem in merged:
        #pprint(elem)
        channel_response = flattenAndParseChannelResponse(elem)
        if channel_response.get('sub_count') != 0:
            dict_list.append(channel_response)

    return dict_list, youtube2



def get_channels_from_channel_ids(conn,channel_ids_list: List[str]):
    channel_ids = ','.join(channel_ids_list)
    youtube = init_yt_client(conn)
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_ids
    )

    response = request.execute()
    merged = get_all_items_from_response(response)
    channel_response = flattenAndParseChannelResponse(merged)
    return channel_response


def get_most_popular_in_all_categories(youtube, num_pages, num_items_per_page=30, video=False):
    cat_ids = ['1',
               '2',
               '10']
    # cat_ids = list(get_category_ids_from_file())
    request_response_array_pair_list = []

    for id in cat_ids:
        search_params_most_popular = dict(maxResults=num_items_per_page, part='snippet,contentDetails,statistics',
                                          chart="mostPopular", pageToken="CDIQAA",
                                          regionCode="US", videoCategoryId=id)
        request_array, response_array = makeSearchRequestsForNRecordsClean(youtube, num_pages,
                                                                           search_params_most_popular, True)
        print("Request array")
        #pprint(request_array)
        print("Response array")
        #pprint(response_array)
        request_response_array_pair_list.append((request_array, response_array))
    return request_response_array_pair_list


def main():
    conn = init_db_connection()
    test = ['UCijjpfm0MvYAacCXopzBr5Q',
            'UCZCD9loEW85QvsjwqC17SZQ',
            'UC_pumopNzv0mMK5ggFfnmoQ',]
    #a = get_channel_from_channel_id(test)
    a = get_channel_from_channel_id(test)
    pprint(a)
    #a = get_channel_from_channel_id(test)
    #pprint(a)

    write_dictlist_to_db(conn, a, table_name_channels)
    # x = get_channel_from_channel_id(['UC_x5XG1OV2P6uZZ5FSM9Ttw','UCDEW2psJxrMGqvjv-xTSspQ','','',''])
    # pprint(x)


if __name__ == "__main__":
    main()
