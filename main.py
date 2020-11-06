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

scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
from helper_functions import append_dictlist_to_csv, write_dict_to_db, update_export_status, addResponseToDB, \
    load_db_table_unexported, init_db_connection, write_dictlist_to_csv


def flattenAndParseChannelResponse(response):
    channel_id = response.get('id')
    kind = response.get('kind')
    snippet = response.get('snippet')
    stats = response.get('statistics')
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
    engagement = 0
    channel_data = {
        'channel_id': channel_id,
        'title': title,
        'sub_count': sub_count,
        'video_count': video_count,
        'view_count': view_count,
        'engagement': engagement,
        'country': country,
        'exported_already': False,
        'data_of_export': None
    }
    curr_dict = channel_data

    return curr_dict


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


def makeSearchRequestsForNRecordsClean(youtube, num_pages, search_params, video=False):
    '''sample response with the following params stored in
     Search params used: search_params_most_popular = dict(maxResults=5, part='snippet,contentDetails,statistics',
                                       chart="mostPopular", pageToken="CDIQAA",
                                      regionCode="US")
     Function call made:
     request_array,response_array = makeSearchRequestsForNRecordsClean(youtube, 2, search_params_most_popular, True)

     '''
    request_array = []
    response_array = []
    next_page_token = 'a'
    page_token_array = []

    def create_request(**kwargs):
        if (video == False):
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
        print("HTTP Error. Nothing written. Aborting")
        return request_array, response_array
    return request_array, response_array


def initializeYoutubeClient():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_key = 'AIzaSyA5jSfrY1m1iCEbPR3zW1U3Eo_dg0ecf08' #"AIzaSyBohPB-s5xjrjtjhB1iuIBfQpiFZxc4d2o"  # "AIzaSyA5jSfrY1m1iCEbPR3zW1U3Eo_dg0ecf08"  # "AIzaSyBohPB-s5xjrjtjhB1iuIBfQpiFZxc4d2o" #"AIzaSyA5jSfrY1m1iCEbPR3zW1U3Eo_dg0ecf08" # "AIzaSyBohPB-s5xjrjtjhB1iuIBfQpiFZxc4d2o" #"AIzaSyDLT1w9RuCvXpeY3i6CRiM7gPD4Vf_0VEI" # AIzaSyAxJwOIw91bTDYHXaIcLbZzT8rqVLovt-k AIzaSyDLT1w9RuCvXpeY3i6CRiM7gPD4Vf_0VEI
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "client_secret_1080473066558-rh5ihim77tc3qbpvparpjnts926tuk3t.apps.googleusercontent.com.json"

    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube


def dispatch_response(kind,response):
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



def flattenAndParseResponses(response, video=False):


    kind = response.get('kind')



    parsed_response = dispatch_response(kind,response)

    return parsed_response


def flattenAndParseVideoResponse(response):
    vid_id = response.get('id')
    snippet = response.get('snippet')
    channel_id = snippet.get('channelId')
    vid_title = snippet.get('title')
    vid_description = snippet.get('description')
    channel_title = snippet.get('title')
    stats = response.get('statistics')
    viewCount = stats.get('viewCount')
    video_flattened = {
        'video_id': vid_id,
        'channel_id': channel_id,
        'video_title': vid_title,
        'description': vid_description,
        'channel_title': channel_title,
        'view_count': viewCount,
        'exported_already': False,
        'date_of_export': None
    }
    return video_flattened


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


def get_videos_from_channel(youtube, channel_id):
    pass


def getChannelOrVidResponsesFromSearchResponse(youtube, search_response):
    # search_response.get('items')
    responses = []
    for item in search_response.get('items'):
        id = item.get('id')
        if id is not None:
            channel_id = id.get('channelId')
            video_id = id.get('videoId')
            request = ""
            response = ""
            if channel_id is not None:
                request = youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
                )
                response = request.execute()
                responses.append(response)
            elif video_id is not None:
                request = youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=video_id
                )
                response = request.execute()
                responses.append(response)
            else:
                print("Can't convert search response")
                # might need to add some error handling in code in case something is up

    return responses


def getChannelOrVidResponsesFromSearchResponse2(youtube, item):
    # search_response.get('items')
    responses = []
    id = item.get('id')
    if id is not None:
        channel_id = id.get('channelId')
        video_id = id.get('videoId')
        pprint(video_id)
        pprint(channel_id)
        request = ""
        response = ""
        if channel_id is not None:
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id
            )
            response = request.execute()
            responses.append(response)
        elif video_id is not None:
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            response = request.execute()
            responses.append(response)
        else:
            print("Can't convert search response")
            # might need to add some error handling in code in case something is up

    return responses


def determine_table_from_kind():
    pass


def main_postgres_clean_channels_workflow():
    youtube = initializeYoutubeClient()
    connection = psycopg2.connect(user="postgres",
                                  password="test",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")

    search_params_most_popular = dict(maxResults=5, part='snippet,contentDetails,statistics',
                                      chart="mostPopular", pageToken="CDIQAA",
                                      regionCode="US")
    search_params = dict(maxResults=50, order='viewCount', part='snippet', type='channel',
                         pageToken="CDIQAA", q='garden')

    request_array, response_array = makeSearchRequestsForNRecordsClean(youtube, 4, search_params)
    converted_response_array = []
    for response in response_array:
        converted_request = getChannelOrVidResponsesFromSearchResponse(youtube, response)
        converted_response_array.append(converted_request)

    print("CONVERTED REQUEST ARRAY")
    pprint(converted_response_array[0])
    print(len(converted_response_array[0]))
    pprint(converted_response_array[0])
    print("TEST TEST")
    pprint(converted_response_array[0][0])
    pprint("Converted Response Array Full")
    pprint(converted_response_array)
    merged = list(itertools.chain.from_iterable(converted_response_array))
    parsed_flattened_arr = []
    for elem in merged:
        parsed_response2 = flattenAndParseResponses(elem)
        parsed_flattened_arr.append(parsed_response2)
        print("flattened response stuff")
        pprint(parsed_response2)
    print("Converted response array flattened")
    pprint(merged)

    print("PARSED RESPONSES")
    pprint(parsed_flattened_arr)
    parsed_flattened_arr = list(itertools.chain.from_iterable(parsed_flattened_arr))
    print("flat flat flat flat")
    pprint(parsed_flattened_arr)
    for response in parsed_flattened_arr:
        addResponseToDB(connection, response)
    # cursor = connection.cursor()
    # cursor.execute('INSERT INTO "channels" VALUES (\'UCEuOwB9vSL1oPKGNdONB4ig\',\'Red Hot Chili '
    # "Peppers','5590000','139','4296643902',768.6303939177102,'US')")
    channels = Table('channels')
    q = Query.from_(channels).select(channels.star)
    pprint(q.get_sql())
    final_dict = load_db_table_unexported(connection, 'channels')
    update_export_status(connection, parsed_flattened_arr, 'TRUE')
    if (connection):
        # cursor.close()
        connection.commit()
        connection.close()
        print("PostgreSQL connection is closed")
    chosen_date = datetime.today().strftime('%Y-%m-%d')
    # yesterday = date.today() - timedelta(days=1)
    # chosen_date = yesterday.strftime('%Y-%m-%d')
    top_users = "analysis_" + 'youtube' + "_" + chosen_date + ".csv"
    path_to_write_to = os.path.join('data', top_users)

    append_dictlist_to_csv(final_dict, path_to_write_to)
    """
    parsed_responses = flattenAndParseResponses(response_array)
    print("Parsed Responses:")
    pprint(parsed_responses)
    # merged = list(itertools.chain.from_iterable(parsed_responses))
    
    """


def make_embed_url(list_of_vid_responses):
    # https://www.youtube.com/watch?v=123456 to https://www.youtube.com/embed/123456
    for elem in list_of_vid_responses:
        vid_id = elem.get('video_id')
        if vid_id is not None:
            embed_url = "https://www.youtube.com/embed/" + str(vid_id)
            elem['embed_url'] = embed_url
    return list_of_vid_responses


def main_postgres_clean_vids_workflow2():
    youtube = initializeYoutubeClient()
    connection = psycopg2.connect(user="postgres",
                                  password="test",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")

    search_params_most_popular = dict(maxResults=20, part='snippet,contentDetails,statistics',
                                      chart="mostPopular", pageToken="CDIQAA",
                                      regionCode="US")
    search_params = dict(maxResults=5, order='viewCount', part='snippet', type='views',
                         pageToken="CDIQAA")

    request_array, response_array = makeSearchRequestsForNRecordsClean(youtube, 2, search_params, False)
    converted_response_array = response_array
    # pprint(converted_response_array)
    for response in response_array:
        result = isinstance(response, list)
        if result is not True:
            items = response.get('items')
            for item in items:
                converted_request = getChannelOrVidResponsesFromSearchResponse2(youtube, item)
                if converted_request:
                    result2 = isinstance(converted_request, list)
                    if not result2:
                        converted_response_array.append(converted_request)

    """
    print("CONVERTED REQUEST ARRAY")
    pprint(converted_response_array[0])
    print(len(converted_response_array[0]))
    pprint(converted_response_array[0])
    print("TEST TEST")
    pprint(converted_response_array[0][0])
    pprint("Converted Response Array Full")
    pprint(converted_response_array)
    """

    print("CONVERTED")
    pprint(converted_response_array)
    print("END CONVERTED")
    # merged = list(itertools.chain.from_iterable(converted_response_array))
    merged = []
    for elem in response_array:
        result = isinstance(elem, list)
        # pprint(result)
        if result is not True:
            merged.append(elem.get('items'))
        else:
            pprint(result)
    merged = list(itertools.chain.from_iterable(merged))
    print('MERGED')
    pprint(merged)
    parsed_flattened_arr = []

    for elem in merged:
        parsed_response2 = flattenAndParseResponses(elem, True)
        parsed_flattened_arr.append(parsed_response2)
        print("flattened response stuff")
        pprint(parsed_response2)
    print("Converted response array flattened")
    pprint(merged)

    print("PARSED RESPONSES")
    pprint(parsed_flattened_arr)
    parsed_flattened_arr = list(itertools.chain.from_iterable(parsed_flattened_arr))
    print("flat flat flat flat")
    pprint(parsed_flattened_arr)
    for response in parsed_flattened_arr:
        addResponseToDB(connection, response)

    channels = Table('channels')
    q = Query.from_(channels).select(channels.star)
    pprint(q.get_sql())
    final_dict = load_db_table_unexported(connection, 'videos')
    update_export_status(connection, parsed_flattened_arr, 'TRUE')
    final_dict = make_embed_url(final_dict)
    if (connection):
        # cursor.close()
        connection.commit()
        connection.close()
        print("PostgreSQL connection is closed")

    chosen_date = datetime.today().strftime('%Y-%m-%d')
    # yesterday = date.today() - timedelta(days=1)
    # chosen_date = yesterday.strftime('%Y-%m-%d')
    top_posts = "top_" + 'youtube' + "_" + chosen_date + ".csv"

    path_to_write_to = os.path.join('test_data', top_posts)
    append_dictlist_to_csv(final_dict, path_to_write_to)


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


def main_postgres_clean_vids_workflow():
    youtube = initializeYoutubeClient()
    connection = psycopg2.connect(user="postgres",
                                  password="test",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")
    search_params_most_popular = dict(maxResults=20, part='snippet,contentDetails,statistics',
                                      chart="mostPopular", pageToken="CDIQAA",
                                      regionCode="US")
    search_params = dict(maxResults=50, order='viewCount', part='snippet', type='channel',
                         pageToken="CDIQAA")
    request_response_array_array1 = get_most_popular_in_all_categories(youtube, 8, 20, True)  #
    request_response_array_array2 = makeSearchRequestsForNRecordsClean(youtube, 8, search_params_most_popular, True)
    request_response_array_array = request_response_array_array1 + request_response_array_array2
    for elem in request_response_array_array:
        request_array = elem[0]
        response_array = elem[1]
        merged = []
        for elem in response_array:
            merged.append(elem.get('items'))
        merged = list(itertools.chain.from_iterable(merged))

        parsed_flattened_arr = []
        for elem in merged:
            parsed_response2 = flattenAndParseResponses(elem, True)
            parsed_flattened_arr.append(parsed_response2)

        parsed_flattened_arr = list(itertools.chain.from_iterable(parsed_flattened_arr))


        for response in parsed_flattened_arr:
            addResponseToDB(connection, response)

        channels = Table('channels')
        q = Query.from_(channels).select(channels.star)
        final_dict = load_db_table_unexported(connection, 'videos')
        update_export_status(connection, parsed_flattened_arr, 'TRUE')
        final_dict = make_embed_url(final_dict)
        chosen_date = datetime.today().strftime('%Y-%m-%d')
        top_posts = "top_" + 'youtube' + "_" + chosen_date + ".csv"
        path_to_write_to = os.path.join('data', top_posts)
        append_dictlist_to_csv(final_dict, path_to_write_to)
    if connection:
        connection.commit()
        connection.close()
        print("PostgreSQL connection is closed")




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

def flatten_and_parse_all_responses(response_array,video=True):
    """@safe | json written to <function_name>.json. It is the result of calling this function after
       get_all_items_from_response with sample response listed at that function.
        """
    parsed_flattened_arr = []
    for elem in response_array:
        parsed_response2 = flattenAndParseResponses(elem, video)
        parsed_flattened_arr.append(parsed_response2)
    return parsed_flattened_arr

def flatten_and_parse_all_responses_channels_test(youtube,response_array,video=False):
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


def get_vids_and_only_write_to_db():
    youtube = initializeYoutubeClient()
    connection = init_db_connection()
    search_params_most_popular = dict(maxResults=5, part='snippet,contentDetails,statistics',
                                      chart="mostPopular", pageToken="CDIQAA",
                                      regionCode="US")
    search_params = dict(maxResults=5, order='viewCount', part='snippet', type='channel',
                         pageToken="CDIQAA")
    #request_array1, response_array1 = get_most_popular_in_all_categories(youtube, 8, 50, True)  #
    request_array2, response_array2 = makeSearchRequestsForNRecordsClean(youtube, 2, search_params_most_popular, True)
    #response_array3 = response_array1 + response_array2
    parsed_flattened_arr = []
    merged = get_all_items_from_response(response_array2)
    parsed_flattened_arr = []
    parsed_flattened_arr = flatten_and_parse_all_responses(merged)
    return parsed_flattened_arr

def get_channels_and_only_write_to_db():
    youtube = initializeYoutubeClient()
    connection = init_db_connection()
    #search_params_most_popular = dict(maxResults=5, part='snippet,contentDetails,statistics',
    #                                  chart="mostPopular", pageToken="CDIQAA",
    #                                  regionCode="US")
    search_params = dict(maxResults=25, order='viewCount', part='snippet', type='channel',
                         pageToken="CDIQAA", q='garden')
    # request_array1, response_array1 = get_most_popular_in_all_categories(youtube, 8, 50, True)  #
    request_array2, response_array2 = makeSearchRequestsForNRecordsClean(youtube, 4, search_params, False)
    # response_array3 = response_array1 + response_array2
    parsed_flattened_arr = []
    merged = get_all_items_from_response(response_array2)
    parsed_flattened_arr = []
    parsed_flattened_arr = flatten_and_parse_all_responses_channels_test(youtube,merged,False)
    return parsed_flattened_arr


def main_test():
    youtube = initializeYoutubeClient()
    connection = psycopg2.connect(user="postgres",
                                  password="test",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")

    search_params_most_popular = dict(maxResults=20, part='snippet,contentDetails,statistics',
                                      chart="mostPopular", pageToken="CDIQAA",
                                      regionCode="US")
    search_params = dict(maxResults=50, order='viewCount', part='snippet', type='channel',
                         pageToken="CDIQAA")

    x = get_most_popular_in_all_categories(youtube, 8, 20, True)


def update_category_ids():
    youtube = initializeYoutubeClient()
    request = youtube.videoCategories().list(
        part="snippet",
        regionCode="US"
    )
    response = request.execute()
    id_list = []
    for item in response.get('items'):
        id_list.append(item.get('id') + '\n')
    with open('youtube_category_ids.txt', 'w', encoding='utf-8') as f:
        f.writelines(id_list)


def get_category_ids_from_file(filepath='youtube_category_ids.txt'):
    with open(filepath) as f:
        mylist = f.read().splitlines()
    pprint(mylist)
    return mylist



def main_test_today():
    channels = get_channels_and_only_write_to_db()
    print("Finished")
    pprint(channels)

if __name__ == "__main__":
    #print(len(unexported_channels))
    #main_test_today()
    # main_postgres_clean_channels_workflow()
    # main_test()
    run_exports_temp('data')
    # main_azure()
