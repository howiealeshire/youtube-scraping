import itertools
import random
from pprint import pprint
from typing import List, Any

from helper_functions import init_db_connection
from instagram_dataflows import get_filtered_words
from yt_main import init_yt_client, makeSearchRequestsForNRecordsClean, flattenAndParseChannelResponse, \
    flattenAndParseResponses


def flatten_and_parse_all_responses(response_array, video=True):
    """@safe | json written to <function_name>.json. It is the result of calling this function after
       get_all_items_from_response with sample response listed at that function.
        """
    parsed_flattened_arr = []
    for elem in response_array:
        parsed_response2 = flattenAndParseResponses(elem, video)
        parsed_flattened_arr.append(parsed_response2)
    return parsed_flattened_arr


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


def get_most_popular_vids(regionCode):
    youtube = init_yt_client()
    search_params_most_popular = dict(maxResults=50, part='snippet,contentDetails,statistics',
                                      chart="mostPopular", pageToken="CDIQAA",
                                      regionCode=regionCode)

    request_array2, response_array2 = makeSearchRequestsForNRecordsClean(youtube, 20, search_params_most_popular, True)
    merged = get_all_items_from_response(response_array2)
    parsed_flattened_arr = flatten_and_parse_all_responses(merged)
    return parsed_flattened_arr


def get_most_popular_channels():
    youtube = init_yt_client()
    filtered_words = get_filtered_words()
    chosen_word = random.sample(filtered_words, 5)
    search_params = dict(maxResults=50, order='viewCount', part='snippet', type='channel',
                         pageToken="CDIQAA", q=chosen_word[0])
    request_array2, response_array2 = makeSearchRequestsForNRecordsClean(youtube, 8, search_params, False)
    merged = get_all_items_from_response(response_array2)
    parsed_flattened_arr = flatten_and_parse_all_responses_channels_test(youtube, merged, False)
    return parsed_flattened_arr


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
        pprint(response)
        item = response.get('items')
        if item:
            item = item[0]
        parsed_response2 = flattenAndParseResponses(item, video)
        parsed_flattened_arr.append(parsed_response2)
    return parsed_flattened_arr



def get_channel_from_channel_id(channel_ids_list: List[str]):
    channel_id = ','.join(channel_ids_list)
    youtube = init_yt_client()

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    merged = get_all_items_from_response([response])
    dict_list = []
    for elem in merged:
        channel_response = flattenAndParseChannelResponse(elem)
        dict_list.append(channel_response)

    return dict_list

def get_channels_from_channel_ids(channel_ids_list: List[str]):
    channel_ids = ','.join(channel_ids_list)
    youtube = init_yt_client()
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_ids
    )
    response = request.execute()
    merged = get_all_items_from_response(response)
    channel_response = flattenAndParseChannelResponse(merged)
    return channel_response
    #
    #
    #return channel_response



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
        pprint(request_array)
        print("Response array")
        pprint(response_array)
        request_response_array_pair_list.append((request_array, response_array))
    return request_response_array_pair_list


def main():
    x = get_channel_from_channel_id(['UC_x5XG1OV2P6uZZ5FSM9Ttw','UCDEW2psJxrMGqvjv-xTSspQ','','',''])
    pprint(x)

if __name__ == "__main__":
    main()
