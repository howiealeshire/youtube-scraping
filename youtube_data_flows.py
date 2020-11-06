import random

from helper_functions import init_db_connection
from instagram_dataflows import get_filtered_words
from main import initializeYoutubeClient, makeSearchRequestsForNRecordsClean, get_all_items_from_response, \
    flatten_and_parse_all_responses, flatten_and_parse_all_responses_channels_test


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
