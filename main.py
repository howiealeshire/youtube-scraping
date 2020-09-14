# -*- coding: utf-8 -*-

# Sample Python code for youtube.channels.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/guides/code_samples#python

"""High level goal: get total number of followers to video views ratio
   Calculate:
        1.) Get total number of subscribers for a channel
        2.) Get total number of views for all videos combined
        3.) Get average of all views per vid
        4.) average views/subscriber count
"""
import csv
from pprint import pprint
import os
import xlsxwriter

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors

scopes = ["https://www.googleapis.com/auth/youtube.readonly"]


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


def flattenAndParseSearchResponse(response):
    #pprint(response)
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
        #channel_title = elem.get('channelTitle')
        commentCount = stats.get('commentCount')
        hidden_sub_count = stats.get('hiddenSubscriberCount')
        sub_count = stats.get('subscriberCount')
        video_count = stats.get('videoCount')
        view_count = stats.get('viewCount')
        channel_data = {
            #'id': channel_id,
            #'kind': kind,
            #'country': country,
            #'publishedAt': publish_date,
            #'commentCount': commentCount,
            #'hiddenSubscriberCount': hidden_sub_count,

            #'videoCount': video_count,
            'Title': title,
            'Subscriber Count': sub_count,
            'View Count': view_count,
            #'customUrl': customUrl,

            #'channelTitle': channel_title,
            'Engagement': int(view_count)/int(sub_count)
        }
        curr_dict = channel_data
        #list_of_dicts.append(channel_data)
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
        #channel_id_list.append(channel_id)
    return channel_id_list

def requestChannelInfosFromChannelIDList(youtube,channel_ids):
    response_list = []
    for elem in channel_ids:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=elem.get('channelId')
        )
        response = request.execute()
        current_dict = {
            'response':response,
            'channelTitle': elem.get('channelTitle')
        }
        response_list.append(current_dict)
    return response_list



def write_dictlist_to_csv(dict_list):
    with open('output.csv', 'w', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(output_file,
                            fieldnames=dict_list[0].keys(),

                            )
        fc.writeheader()
        fc.writerows(dict_list)


def main():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_key = "AIzaSyDLT1w9RuCvXpeY3i6CRiM7gPD4Vf_0VEI"
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "client_secret_1080473066558-rh5ihim77tc3qbpvparpjnts926tuk3t.apps.googleusercontent.com.json"

    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    #request = youtube.channels().list(
    #    part="snippet,contentDetails,statistics",
    #    id="UCGIY_O-8vW4rfX98KlMkvRg"
    #)

    # request = youtube.subscriptions().list(
    #    part="snippet,contentDetails",
    #    id="UC_x5XG1OV2P6uZZ5FSM9Ttw"
    # )
    request = youtube.search().list(
        maxResults=25, order='viewCount', part='snippet', type='channel'
    )
    response = request.execute()
    #response_items = response.get('items')
    pprint(response)
    #response = request.execute()
    channel_ids = getChannelListFromSearchResults(response)
    #pprint(channel_ids)
    channel_infos = requestChannelInfosFromChannelIDList(youtube,channel_ids)
    #print(channel_infos)

    #flattened_results = flattenAndParseSearchResponse(channel_infos)
    flat_list = []
    for elem in channel_infos:
        flattened_results = flattenAndParseSearchResponse(elem)
        flat_list.append(flattened_results)
    pprint(flat_list)
    #print(flattened_results)
    write_dictlist_to_csv(flat_list)

if __name__ == "__main__":
    main()
