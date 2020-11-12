# -*- coding: utf-8 -*-

import csv
import json
import sys
from os import listdir
from os.path import isfile, join
from pprint import pprint
import os, uuid
from typing import List, Any

import psycopg2
import psycopg2.extras
import googleapiclient.discovery
import googleapiclient.errors
import itertools
from pypika import Query, Table, Field, PostgreSQLQuery
from datetime import datetime
from export import main_azure, run_exports_temp
from table_data import *
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
from helper_functions import get_unused_yt_api_key

def makeSearchRequestsForNRecordsClean(youtube, num_pages, search_params, video=False):
    """sample response with the following params stored in
     Search params used: search_params_most_popular = dict(maxResults=5, part='snippet,contentDetails,statistics',
                                       chart="mostPopular", pageToken="CDIQAA",
                                      regionCode="US")
     Function call made:
     request_array,response_array = makeSearchRequestsForNRecordsClean(youtube, 2, search_params_most_popular, True)
     """
    request_array = []
    response_array = []
    next_page_token = 'a'
    page_token_array = []

    def create_request(**kwargs):
        if not video:
            return youtube.search().list(**kwargs)
        else:
            return youtube.videos().list(**kwargs)

    try:
        while num_pages >= 1 and next_page_token:
            request = create_request(**search_params)
            request_array.append(request)
            response = request.execute()
            response_array.append(response)
            next_page_token = response.get('nextPageToken')
            page_token_array.append(next_page_token)
            request_array.append(request)
            num_pages -= 1

    except googleapiclient.errors.HttpError as e:
        print(str(e))
        print(repr(e))
        print("HTTP Error. Nothing written.")
        return request_array, response_array
    return request_array, response_array

def init_yt_client():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_key = get_unused_yt_api_key('youtube_api_keys_unused.txt', 'youtube_api_keys_used.txt')
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "client_secret_1080473066558-rh5ihim77tc3qbpvparpjnts926tuk3t.apps.googleusercontent.com.json"

    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube

def flattenAndParseChannelResponse(response) -> YTChannel:
            channel_id = response.get('id')
            snippet = response.get('snippet')
            stats = response.get('statistics')
            country = snippet.get('country')
            title = snippet.get('title')
            sub_count = stats.get('subscriberCount')
            video_count = stats.get('videoCount')
            view_count = stats.get('viewCount')

            return YTChannel(channel_id, title, sub_count, video_count, view_count, country)

def flattenAndParseSearchResponse(response):
            pprint(response)
            channel_id = response.get('id').get('channelId')
            snippet = response.get('snippet')
            stats = response.get('statistics')
            country = snippet.get('country')
            title = snippet.get('title')
            sub_count = stats.get('subscriberCount')
            video_count = stats.get('videoCount')
            view_count = stats.get('viewCount')
            engagement = 0
            channel_data = {
                'kind': response.get('kind'),
                'channel_id': channel_id,
                'title': title,
                'sub_count': sub_count,
                'video_count': video_count,
                'view_count': view_count,
                'engagement': engagement,
                'country': country,
                'exported_already': False,
                'date_of_export': None
            }
            curr_dict = channel_data

            return curr_dict

def flattenAndParseVideoResponse(response) -> YTVideo:
            vid_id = response.get('id')
            snippet = response.get('snippet')
            channel_id = snippet.get('channelId')
            vid_title = snippet.get('title')
            vid_description = snippet.get('description')
            channel_title = snippet.get('title')
            stats = response.get('statistics')
            viewCount = stats.get('viewCount')
            return YTVideo(vid_id, channel_id, vid_title, vid_description, channel_title, viewCount)

def flattenAndParseVideoListResponse(response):
            print("RESPONSE")
            pprint(response)
            print("RESPONSE ITEMS")
            items = response.get('items')
            pprint(items)
            if items is None:
                print("whoops, items is none")
                sys.exit()
            flattened_video_list = []
            for elem in items:
                flattened_video = flattenAndParseVideoResponse(elem)
                flattened_video_list.append(flattened_video)

            return flattened_video_list

def flattenAndParseResponses(response, video=False):
    kind = response.get('kind')

    def dispatch_response(kind, response):
        parsed_response = []



        if response is not None:
            if kind == "youtube#searchListResponse":
                parsed_response = flattenAndParseSearchResponse(response.get('items'))
            elif kind == "youtube#channelListResponse":
                parsed_response = flattenAndParseChannelResponse(response.get('items'))
            elif kind == "youtube#videoListResponse":
                parsed_response = flattenAndParseVideoResponse(response.get('items'))
            elif kind == "youtube#video":
                parsed_response = flattenAndParseVideoResponse(response)  # good to go for db
            elif kind == "youtube#channel":
                parsed_response = flattenAndParseChannelResponse(response)  # good
            elif kind == "youtube#searchResult":
                parsed_response = flattenAndParseSearchResponse(response)
            else:
                print("No known kind: " + kind)

        return parsed_response

    parsed_response = dispatch_response(kind, response)

    return parsed_response

def main():
    pass

if __name__ == "__main__":
    main()

