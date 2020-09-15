# -*- coding: utf-8 -*-

# Sample Python code for youtube.channels.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/guides/code_samples#python

import csv
from pprint import pprint
import os
import xlsxwriter

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors

scopes = ["https://www.googleapis.com/auth/youtube.readonly"]


def throwawaycode():
    # request = youtube.channels().list(
    #    part="snippet,contentDetails,statistics",
    #    id="UCGIY_O-8vW4rfX98KlMkvRg"
    # )

    # request = youtube.subscriptions().list(
    #    part="snippet,contentDetails",
    #    id="UC_x5XG1OV2P6uZZ5FSM9Ttw"
    # )
    """
    flat_list = []
    for elem in channel_infos:
        flattened_results = flattenAndParseSearchResponse(elem)
        flat_list.append(flattened_results)
    pprint(flat_list)
    """
    """
    channel_ids = getChannelListFromSearchResults(response)
    channel_infos = requestChannelInfosFromChannelIDList(youtube, channel_ids)
    write_dictlist_to_csv(flat_list)
    """
    pass


def getNChannelsWithTopViewedVids(youtube, num_results):
    request = youtube.search().list(
        part="snippet",
        maxResults=num_results,
        order="viewCount",
        type="channel",
    )
    return request


def writeResponseToFile(response, filepath):
    pass

def retrieve_all_page_tokens(filepath):
    with open(filepath) as f:
        return f.read().splitlines()

def get_unused_tokens(used_tokens_path,all_tokens_path):
    used_tokens = []
    all_tokens = []
    with open(used_tokens_path) as f:
        used_tokens = f.read().splitlines()
    with open(all_tokens_path) as f:
        all_tokens = f.read().splitlines()
    return set(all_tokens) - set(used_tokens)


def flattenAndParseSearchResponse(response):
    # pprint(response)
    channel_title = response.get('channelTitle')
    response = response.get('response')
    items = response.get('items')
    curr_dict = {}
    for elem in items:
        channel_id = elem.get('id')
        kind = elem.get('kind')
        snippet = elem.get('snippet')
        stats = elem.get('statistics')
        country = snippet.get('country')
        customUrl = snippet.get('customUrl')
        publish_date = snippet.get('publishedAt')
        title = snippet.get('title')
        # channel_title = elem.get('channelTitle')
        commentCount = stats.get('commentCount')
        hidden_sub_count = stats.get('hiddenSubscriberCount')
        sub_count = stats.get('subscriberCount')
        video_count = stats.get('videoCount')
        view_count = stats.get('viewCount')
        if int(view_count) == 0:
            break
        engagement = 0
        if int(sub_count) > 0:
            engagement = int(view_count) / int(sub_count)
        else:
            break
        channel_data = {
            # 'id': channel_id,
            # 'kind': kind,
            # 'country': country,
            # 'publishedAt': publish_date,
            # 'commentCount': commentCount,
            # 'hiddenSubscriberCount': hidden_sub_count,

            # 'videoCount': video_count,
            'Title': title,
            'Subscriber Count': sub_count,
            'View Count': view_count,
            # 'customUrl': customUrl,

            # 'channelTitle': channel_title,
            'Engagement': engagement

        }
        curr_dict = channel_data
        # list_of_dicts.append(channel_data)
    return curr_dict


def getChannelListFromSearchResults(results):
    items = results.get('items')
    channel_id_list = []
    for elem in items:
        snippet = elem.get('snippet')
        channel_id = snippet.get('channelId')
        channel_title = snippet.get('channelTitle')
        current_dict = {
            'channelId': channel_id,
            'channelTitle': channel_title
        }
        channel_id_list.append(current_dict)
        # channel_id_list.append(channel_id)
    return channel_id_list


def requestChannelInfosFromChannelIDList(youtube, channel_ids):
    response_list = []
    for elem in channel_ids:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=elem.get('channelId')
        )
        response = request.execute()
        current_dict = {
            'response': response,
            'channelTitle': elem.get('channelTitle')
        }
        response_list.append(current_dict)
    return response_list


def write_dictlist_to_csv(dict_list):
    with open('output.csv', 'a', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(output_file,
                            fieldnames=dict_list[0].keys(),

                            )
        fc.writeheader()
        fc.writerows(dict_list)

def get_used_tokens(used_tokens_path):
    with open(used_tokens_path) as f:
        used_tokens = f.read().splitlines()
        return used_tokens

def makeSearchRequestsForNRecords(youtube, n, tokens_to_use=[],used_tokens=[]):
    request_array = []
    response_array = []
    next_page_token = 'a'
    page_token_arr = []
    if(len(used_tokens) != 0):
        token_to_start_today = used_tokens[-1]
    else:
        token_to_start_today = "CDIQAA"
        page_token_arr.append(token_to_start_today)

    i = 0
    while n > 0 and next_page_token is not None:
        if i == 0:
            request = youtube.search().list(
                maxResults=50, order='viewCount', part='snippet', type='channel', pageToken=token_to_start_today
            )
            i += 1
        else:
            request = youtube.search().list(
                maxResults=50, order='viewCount', part='snippet', type='channel', pageToken=next_page_token
            )
            i += 1
        response = request.execute()
        if i > 1:
            response_array.append(response)
        next_page_token = response.get('nextPageToken')
        if i > 1:
            page_token_arr.append(next_page_token)
        if i > 1:
            request_array.append(request)
        n -= 50
    used_tokens = get_used_tokens('used_tokens')
    with open('used_tokens', 'a') as f:
        for item in page_token_arr:
            if item not in used_tokens:
                f.write("%s\n" % item)
    """
    for token in tokens_to_use:
        if len(used_tokens) == 0:
            request = youtube.search().list(
                maxResults=50, order='viewCount', part='snippet', type='channel'
            )
            tokens_used_today.append(token)
            used_tokens.append(token)
        else:
            request = youtube.search().list(
                maxResults=50, order='viewCount', part='snippet', type='channel', pageToken=token
            )
            tokens_used_today.append(token)
        response = request.execute()
        response_array.append(response)
        request_array.append(request)
    with open('used_tokens', 'a') as f:
        for item in tokens_used_today:
            f.write("%s\n" % item)
    """
    """
    
    """
    """
    if n > 0:
        request = youtube.search().list(
            maxResults=50, order='viewCount', part='snippet', type='channel'
        )
        request_array.append(request)
        response_array.append(request.execute())
        n -= 50
    """
    return request_array, response_array




def initializeYoutubeClient():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_key = "AIzaSyAxJwOIw91bTDYHXaIcLbZzT8rqVLovt-k" #"AIzaSyDLT1w9RuCvXpeY3i6CRiM7gPD4Vf_0VEI"
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "client_secret_1080473066558-rh5ihim77tc3qbpvparpjnts926tuk3t.apps.googleusercontent.com.json"

    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube


def makeResponseArrayFromRequestArray(requests_array):
    response_array = []
    for request in requests_array:
        response = request.execute()
        response_array.append(response)
    return response_array


def getChannelListsFromResponseArray(response_array):
    channel_ids_list = []
    for response in response_array:
        channel_ids = getChannelListFromSearchResults(response)
        channel_ids_list.append(channel_ids)
    return channel_ids_list


def requestChannelInfosFromListOfChannelIDList(youtube, list_of_channel_id_list):
    channel_infos_list = []
    for channel_id_list in list_of_channel_id_list:
        channel_infos = requestChannelInfosFromChannelIDList(youtube, channel_id_list)
        channel_infos_list.append(channel_infos)
    return channel_infos_list


def flattenAndParseSearchResponses(channel_infos_list):
    flattened_results = []
    for channel_infos in channel_infos_list:
        for channel_info in channel_infos:
            flattened_result = flattenAndParseSearchResponse(channel_info)
            if flattened_result:
                flattened_results.append(flattened_result)
    return flattened_results



def main():
    youtube = initializeYoutubeClient()
    unused_tokens = get_unused_tokens("used_tokens","youtube_page_tokens")
    used_tokens = get_used_tokens('used_tokens')
    request_array, response_array = makeSearchRequestsForNRecords(youtube,150,[], used_tokens)
    print("request array")
    pprint(request_array)
    print("response array")
    pprint(response_array)
    channel_id_lists = getChannelListsFromResponseArray(response_array)
    channel_infos_list = requestChannelInfosFromListOfChannelIDList(youtube, channel_id_lists)
    flattened_responses = flattenAndParseSearchResponses(channel_infos_list)
    write_dictlist_to_csv(flattened_responses)
#493
def main_test():
    pprint(get_unused_tokens("used_tokens","youtube_page_tokens"))

if __name__ == "__main__":
    main()
