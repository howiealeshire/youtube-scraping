import random
from pprint import pprint
from typing import List

from helper_functions import init_db_connection
from instagram_dataflows import get_filtered_words
from main import initializeYoutubeClient, makeSearchRequestsForNRecordsClean, get_all_items_from_response, \
    flatten_and_parse_all_responses, flatten_and_parse_all_responses_channels_test, flattenAndParseChannelResponse


def get_most_popular_vids(regionCode):
    youtube = initializeYoutubeClient()
    search_params_most_popular = dict(maxResults=50, part='snippet,contentDetails,statistics',
                                      chart="mostPopular", pageToken="CDIQAA",
                                      regionCode=regionCode)

    request_array2, response_array2 = makeSearchRequestsForNRecordsClean(youtube, 20, search_params_most_popular, True)
    merged = get_all_items_from_response(response_array2)
    parsed_flattened_arr = flatten_and_parse_all_responses(merged)
    return parsed_flattened_arr


def get_most_popular_channels():
    youtube = initializeYoutubeClient()
    filtered_words = get_filtered_words()
    chosen_word = random.sample(filtered_words, 5)
    search_params = dict(maxResults=50, order='viewCount', part='snippet', type='channel',
                         pageToken="CDIQAA", q=chosen_word[0])
    request_array2, response_array2 = makeSearchRequestsForNRecordsClean(youtube, 8, search_params, False)
    merged = get_all_items_from_response(response_array2)
    parsed_flattened_arr = flatten_and_parse_all_responses_channels_test(youtube, merged, False)
    return parsed_flattened_arr


def get_channel_from_channel_id(channel_ids_list: List[str]):
    channel_id = ','.join(channel_ids_list)
    youtube = initializeYoutubeClient()

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
    youtube = initializeYoutubeClient()

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

def main():
    x = get_channel_from_channel_id(['UC_x5XG1OV2P6uZZ5FSM9Ttw','UCDEW2psJxrMGqvjv-xTSspQ','','',''])
    pprint(x)

if __name__ == "__main__":
    main()
