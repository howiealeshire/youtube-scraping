# -*- coding: utf-8 -*-

import csv
import sys
from pprint import pprint
import os, uuid

import psycopg2
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

import xlsxwriter

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import itertools
from pypika import Query, Table, Field
from datetime import datetime

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


def retrieve_all_page_tokens(filepath):
    with open(filepath, encoding="utf8") as f:
        return f.read().splitlines()


def get_unused_tokens(used_tokens_path, all_tokens_path):
    used_tokens = []
    all_tokens = []
    with open(used_tokens_path, encoding="utf8") as f:
        used_tokens = f.read().splitlines()
    with open(all_tokens_path, encoding="utf8") as f:
        all_tokens = f.read().splitlines()
    return set(all_tokens) - set(used_tokens)


def flattenAndParseChannelResponse(response):
    # pprint(response)
    print("SHOULD NOT BE NONE")
    pprint(response)
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

            # 'kind': kind,
            # 'country': country,
            # 'publishedAt': publish_date,
            # 'commentCount': commentCount,
            # 'hiddenSubscriberCount': hidden_sub_count,
            'kind': response.get('kind'),
            'Channel Id': channel_id,
            # 'videoCount': video_count,
            'Title': title,
            'Subscriber Count': sub_count,
            'Video Count': video_count,
            'View Count': view_count,
            # 'customUrl': customUrl,

            # 'channelTitle': channel_title,
            'Engagement': engagement,
            'Country': country
        }
        curr_dict = channel_data
        # list_of_dicts.append(channel_data)
    return curr_dict


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
        # query, page_tokens_used, channel_id, order, video_id
        # dictionary will return null for query and page token since I haven't added those fields yet TODO
        channel_data = {
            'kind': kind,
            'query': response.get('query'),
            'page_tokens_used': response.get('page_tokens_used'),
            'id': channel_id,
            'Engagement': engagement
        }
        curr_dict = channel_data
        # list_of_dicts.append(channel_data)
    return curr_dict


def getChannelRequestAndResponse(youtube, channel_id):
    # print(channel_id)
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )

    response = request.execute()
    return request, response


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


def isFirstTimeWritingToOutputFile(path):
    with open(path, encoding="utf8") as f:
        first_line = f.readline()
    if "Title" in first_line:
        return False
    else:
        return True


def removeDuplicateEntriesFromFile(path):
    with open(path, encoding="utf8") as f:
        used_tokens = f.read().splitlines()
    entries = list(used_tokens)
    entries.pop(0)
    entries_split = []
    ids = []
    for elem in entries:
        split_elem = elem.split(',')
        entries_split.append(split_elem)
        ids.append(split_elem[-1])
    temp_entries = entries
    entries_to_delete = []
    entries_to_keep = []
    ids = list(set(ids))
    for idx, elem in enumerate(entries_split):
        if elem[-1] in ids:
            ids.remove(elem[-1])
            if (len(entries) > idx):
                entries_to_keep.append(entries.pop(idx))
        else:
            if (len(entries) > idx):
                entries_to_delete.append(entries[idx])
    entries_to_keep = set(entries_to_keep)
    with open('output_temp_without_dupes.csv', 'w', encoding="utf8") as f:
        for item in entries_to_keep:
            f.write("%s\n" % item)
    with open('dupes.csv', 'w', encoding="utf8") as f:
        for item in entries_to_delete:
            f.write("%s\n" % item)
    pass


def write_dictlist_to_csv(dict_list):
    if (len(dict_list) > 0):
        with open('output_temp.csv', 'a', encoding='utf8', newline='') as output_file:
            fc = csv.DictWriter(output_file,
                                fieldnames=dict_list[0].keys(),

                                )
            if isFirstTimeWritingToOutputFile('output_temp.csv'):
                fc.writeheader()
            fc.writerows(dict_list)
    else:
        print("No items to write. Aborting.")
        sys.exit()


def get_used_tokens(used_tokens_path):
    with open(used_tokens_path, encoding="utf8") as f:
        used_tokens = f.read().splitlines()
        return used_tokens


def get_token_to_start_with(path):
    with open(path, encoding="utf8") as f:
        token_to_start_with = f.read().splitlines()
        return token_to_start_with[0]


def makeSearchRequestsForNRecords(youtube, n, used_tokens=[], channel_id="", most_popular=False):
    request_array = []
    response_array = []
    next_page_token = 'a'
    page_token_arr = []
    total_results = -1
    num_items_this_request = 0
    first_time = False
    if len(used_tokens) == 0:
        first_time = True

    if len(used_tokens) != 0:
        token_to_start_today = get_token_to_start_with('token_to_start_with_temp')
    else:
        token_to_start_today = "CDIQAA"

    i = 0
    donotappend = False
    try:
        while n > 0 and next_page_token is not None:
            if i == 0:
                if most_popular == False:
                    request = youtube.search().list(
                        maxResults=50, order='viewCount', part='snippet', type='channel', pageToken=token_to_start_today
                    )
                else:
                    request = youtube.videos().list(
                        maxResults=50, part='snippet,contentDetails,statistics', pageToken=token_to_start_today,
                        chart="mostPopular",
                        regionCode="US"
                    )
                i += 1
            else:
                if most_popular == False:
                    request = youtube.search().list(
                        maxResults=50, order='viewCount', part='snippet', type='channel', pageToken=next_page_token
                    )
                else:
                    request = youtube.videos().list(
                        maxResults=50, part='snippet,contentDetails,statistics', pageToken=next_page_token,
                        chart="mostPopular",
                        regionCode="US"
                    )

                i += 1

            response = request.execute()
            pprint(response)
            if i == 1:
                pageInfo = response.get('pageInfo')
                total_results = pageInfo.get('totalResults')
                results_per_page = pageInfo.get('resultsPerPage')
                print("Total Results: ")
                print(total_results)
                print("Results per page: ")
                print(results_per_page)
            num_items_this_request = len(response.get('items'))
            if num_items_this_request == 0:
                print("No more items this request")
                print("Offending page marker: ")
                if i == 1:
                    print("Todays token is bad")
                    print(token_to_start_today)
                else:
                    print("next page token is bad")
                    print(next_page_token)
                donotappend = True
            if num_items_this_request != 0 and num_items_this_request != 50:
                print("Strange number of items")
                print(num_items_this_request)
            print("Typical items per request:")
            print(num_items_this_request)
            if i == 1 and first_time:
                response_array.append(response)
            if i > 1:
                response_array.append(response)

            next_page_token = response.get('nextPageToken')
            if i == 1 and first_time and donotappend == False:
                page_token_arr.append(token_to_start_today)
            if i > 1 and donotappend == False:
                page_token_arr.append(next_page_token)
            if i == 1 and first_time:
                request_array.append(request)
            if i > 1:
                request_array.append(request)
            if i > 1:
                n -= num_items_this_request
        used_tokens = get_used_tokens('used_tokens')
        if (len(page_token_arr) > 0):
            token_to_start_next_time = page_token_arr[-1]
        if donotappend == False and token_to_start_next_time is not None:
            with open('token_to_start_with_temp', 'w', encoding="utf8") as the_file:
                the_file.write(token_to_start_next_time)
        if donotappend == False and len(page_token_arr) > 0:
            with open('used_tokens_temp', 'a', encoding="utf8") as f:
                for item in page_token_arr:
                    if item not in used_tokens:
                        f.write("%s\n" % item)

    except googleapiclient.errors.HttpError:
        print("HTTP Error. Nothing written. Aborting")
        sys.exit()

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


def makeSearchRequestsForNRecordsClean(youtube, num_pages, search_params):
    request_array = []
    response_array = []
    next_page_token = 'a'

    def create_request(**kwargs):
        return youtube.search().list(**kwargs)

    try:
        while num_pages >= 1 and next_page_token:
            request = create_request(**search_params)
            request_array.append(request)
            response = request.execute()
            response_array.append(response)
            pprint(response)
            next_page_token = response.get('nextPageToken')
            request_array.append(request)
            num_pages -= 1
    except googleapiclient.errors.HttpError:
        print("HTTP Error. Nothing written. Aborting")
        sys.exit()
    return request_array, response_array


def initializeYoutubeClient():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_key = "AIzaSyDLT1w9RuCvXpeY3i6CRiM7gPD4Vf_0VEI"  # "AIzaSyBohPB-s5xjrjtjhB1iuIBfQpiFZxc4d2o" #"AIzaSyA5jSfrY1m1iCEbPR3zW1U3Eo_dg0ecf08" # "AIzaSyBohPB-s5xjrjtjhB1iuIBfQpiFZxc4d2o" #"AIzaSyDLT1w9RuCvXpeY3i6CRiM7gPD4Vf_0VEI" # AIzaSyAxJwOIw91bTDYHXaIcLbZzT8rqVLovt-k AIzaSyDLT1w9RuCvXpeY3i6CRiM7gPD4Vf_0VEI
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


def flattenAndParseChannelResponses(channel_infos_list):
    flattened_results = []
    for channel_info in channel_infos_list:

        if channel_info is not None:
            pprint(channel_info)
            flattened_result = flattenAndParseChannelResponse(channel_info)
        if flattened_result:
            flattened_results.append(flattened_result)

    return flattened_results


def flattenAndParseVideoListResponses(video_lists):
    flattened_results = []
    for channel_infos in video_lists:
        flattened_result = flattenAndParseVideoListResponse(channel_infos)
        if flattened_result:
            flattened_results.append(flattened_result)

    return flattened_results


def flattenAndParseSearchListResponse(response):
    pass


def flattenAndParseChanneListResponse(response):
    pass


def flattenAndParseResponses(responses):
    parsed_responses = []
    for response in responses:
        kind = response.get('kind')
        if kind == "youtube#searchListResponse":
            parsed_response = flattenAndParseSearchResponse(response.get('items'))
            parsed_responses.append(parsed_response)
        elif kind == "youtube#channelListResponse":
            parsed_response = flattenAndParseChannelResponse(response.get('items'))
            parsed_responses.append(parsed_response)
        elif kind == "youtube#videoListResponse":
            parsed_response = flattenAndParseVideoResponse(response.get('items'))
            parsed_responses.append(parsed_response)
        elif kind == "youtube#video":
            parsed_response = flattenAndParseVideoResponse(response)  # good to go for db
            parsed_responses.append(parsed_response)
        elif kind == "youtube#channel":
            parsed_response = flattenAndParseChannelResponse(response)  # good
            parsed_responses.append(parsed_response)
        elif kind == "youtube#searchResult":
            parsed_response = flattenAndParseSearchResponse(response)
            parsed_responses.append(parsed_response)
        else:
            print("No known kind: " + kind)
    return parsed_responses


def addResponseToDB(connection, response):
    cursor = connection.cursor()
    kind = response.get('kind')
    pg_query = ""
    pg_table = ""

    if kind == "youtube#video":
        pg_table = Table('videos')
        pass
    elif kind == "youtube#channel":
        pg_table = Table('channels')
        pass
    elif kind == "youtube#searchResult":
        pg_table = Table('searches')
    else:
        print("No known kind: " + kind)

    if pg_table != "":
        # does the args splitting work here? must check later.
        response.pop('kind')
        pg_query = pg_table.insert(*response)
        cursor.execute(pg_query.get_sql())


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
        'kind': response.get('kind'),
        'Video Id': vid_id,
        'Channel Id': channel_id,
        'Video Title': vid_id,
        'Video Description': vid_description,
        'Channel Title': channel_title,
        'View Count': viewCount
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

    """
    id = items.get('id')
    snippet = items.get('snippet')
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
        if sub_count is not None and view_count is not None:
            if int(sub_count) > 0:
                engagement = int(view_count) / int(sub_count)
        else:
            break
        channel_data = {

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
            'Engagement': engagement,
            'id': channel_id

        }
        curr_dict = channel_data
        # list_of_dicts.append(channel_data)
        """
    return flattened_video_list


def main():
    channel_id = "UCF0pVplsI8R5kcAqgtoRqoA"
    youtube = initializeYoutubeClient()
    unused_tokens = get_unused_tokens("used_tokens", "youtube_page_tokens")
    used_tokens = get_used_tokens('used_tokens_temp')
    request_array, response_array = makeSearchRequestsForNRecords(youtube, 100, used_tokens, "", True)
    print("request array")
    pprint(request_array)
    print("response array")
    pprint(response_array)
    channel_id_lists = getChannelListsFromResponseArray(response_array)
    channel_infos_list = requestChannelInfosFromListOfChannelIDList(youtube, channel_id_lists)
    flattened_responses = flattenAndParseSearchResponses(channel_infos_list)
    write_dictlist_to_csv(flattened_responses)
    removeDuplicateEntriesFromFile('output_temp.csv')


# 493
def main2():
    # channel_id = "UCF0pVplsI8R5kcAqgtoRqoA"
    youtube = initializeYoutubeClient()
    unused_tokens = get_unused_tokens("used_tokens", "youtube_page_tokens")
    used_tokens = get_used_tokens('used_tokens_temp')
    request_array, response_array = makeSearchRequestsForNRecords(youtube, 5000, [], "", True)
    print(len(response_array))
    flattened_response = flattenAndParseVideoListResponses(response_array)
    pprint(flattened_response)
    channel_set = set()
    for elem in flattened_response:
        # write_dictlist_to_csv(elem)
        for item in elem:
            channel_id = item.get('Channel Id')
            if channel_id is not None:
                channel_set.add(channel_id)
    channel_request_list = []
    channel_response_list = []
    for elem in channel_set:
        channel_request, channel_response = getChannelRequestAndResponse(youtube, elem)
        channel_response_list.append(channel_response)
        channel_request_list.append(channel_request)
    pprint("CHANNEL RESPONSES")
    pprint(channel_response_list)
    flattened_channels = flattenAndParseChannelResponses(channel_response_list)
    print("Flattened Channels")
    pprint(flattened_channels)
    write_dictlist_to_csv(flattened_channels)

    print("request array")
    # pprint(request_array)
    print("response array")
    # pprint(response_array)


def main_test():
    removeDuplicateEntriesFromFile('output_temp.csv')


def list_blobs():
    print("\nListing blobs...")
    container_client = ContainerClient()
    # List the blobs in the container
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        print("\t" + blob.name)


def get_videos_from_channel(youtube, channel_id):
    pass


def main_azure():
    """DefaultEndpointsProtocol=https;
    AccountName=tiktokscraper;
    AccountKey=DJTlaX2lUjgsxZTF6Oy37REj7EUTvLpmxooE/cJUnD3dr48xiFz8RSt8YhZJ6Rbi1yxJ1WNnCyjXR8jYRifDKQ==;
    EndpointSuffix=core.windows.net"""
    try:
        local_path = "data"
        local_file_name = "analysis_youtube_2020-09-17.csv"
        upload_file_path = os.path.join(local_path, local_file_name)

        print("Azure Blob storage v" + __version__ + " - Python quickstart sample")
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        # "analysis_<platform>_<scraped date in YYYY-MM-DD format>.csv" (e.g. analysis_youtube_2020-09-16.csv)
        container_name = "tiktokscraperdata"
        print("Container name: ")
        print(container_name)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
        print(connect_str)
        # Upload the created file
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data)
        # Quick start code goes here
    except Exception as ex:
        print('Exception:')
        print(ex)
    pass


def check_if_dupe_ids():
    lines = []
    with open('output_temp.csv', encoding="utf8") as f:
        lines = f.read().splitlines()
    ids = []
    for elem in lines:
        split_elem = elem.split(',')
        ids.append(split_elem[-1])

    set_lines = set(ids)
    print(len(ids))
    print(len(set_lines))


def main_postgres():
    # conn = psycopg2.connect("dbname=test_table user=postgres password=test")
    connection = psycopg2.connect(user="postgres",
                                  password="test",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")
    cursor = connection.cursor()
    try:

        postgreSQL_select_Query = "select * from test_table"

        cursor.execute(postgreSQL_select_Query)
        print("Selecting rows from mobile table using cursor.fetchall")
        mobile_records = cursor.fetchall()

        print("Print each row and it's columns values")
        for row in mobile_records:
            print("Id = ", row[0], )
            print("Model = ", row[1])
            print("Price  = ", row[2], "\n")

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    pass


def update_export_status(connection, responses):
    pg_table = ''
    cursor = connection.cursor()
    # exported_already, date_of_export
    for response in responses:
        channel_id = response.get('Channel Id')
        video_id = response.get('Video Id')
        if channel_id is not None:
            pg_table = Table('channels')
            query1 = Query.update(pg_table).set(pg_table.exported_already, 'TRUE')
            query2 = Query.update(pg_table).set(pg_table.date_of_export, str(datetime.today().strftime('%Y-%m-%d')))
            cursor.execute(query1.get_sql())
            cursor.execute(query2.get_sql())
        if video_id is not None:
            pg_table = Table('videos')
            query1 = Query.update(pg_table).set(pg_table.exported_already, 'TRUE')
            query2 = Query.update(pg_table).set(pg_table.date_of_export, str(datetime.today().strftime('%Y-%m-%d')))
            cursor.execute(query1.get_sql())
            cursor.execute(query2.get_sql())


def main_postgres_clean():
    youtube = initializeYoutubeClient()
    connection = psycopg2.connect(user="postgres",
                                  password="test",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")
    unused_tokens = get_unused_tokens("used_tokens", "youtube_page_tokens")
    used_tokens = get_used_tokens('used_tokens_temp')
    search_params_most_popular = dict(maxResults=50, part='snippet,contentDetails,statistics',
                                      chart="mostPopular", pageToken="CDIQAA",
                                      regionCode="US")
    search_params = dict(maxResults=50, order='viewCount', part='snippet', type='channel',
                         pageToken="CDIQAA")

    request_array, response_array = makeSearchRequestsForNRecordsClean(youtube, 2, search_params)
    parsed_responses = flattenAndParseResponses(response_array)
    print("Parsed Responses:")
    print(parsed_responses)
    # merged = list(itertools.chain.from_iterable(parsed_responses))
    for response in parsed_responses:
        addResponseToDB(connection, response)

    write_dictlist_to_csv(parsed_responses)
    update_export_status(connection,parsed_responses)

if __name__ == "__main__":
    main_postgres_clean()
