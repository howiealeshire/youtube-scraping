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

from pprint import pprint
import os

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
    # pprint(response)
    items = response['items']
    list_of_dicts = []
    for elem in items:
        channel_id = elem.get('id')
        kind = elem.get('kind')
        snippet = elem.get('snippet')
        stats = elem.get('statistics')
        country = snippet.get('country')
        customUrl = snippet.get('customUrl')
        publish_date = snippet.get('publishedAt')
        title = snippet.get('title')
        commentCount = stats.get('commentCount')
        hidden_sub_count = stats.get('hiddenSubscriberCount')
        sub_count = stats.get('subscriberCount')
        video_count = stats.get('videoCount')
        view_count = stats.get('viewCount')
        channel_data = {
            'id': channel_id,
            'kind': kind,
            'country': country,
            'publishedAt': publish_date,
            'commentCount': commentCount,
            'hiddenSubscriberCount': hidden_sub_count,
            'subscriberCount': sub_count,
            'videoCount': video_count,
            'viewCount': view_count,
            'customUrl': customUrl,
            'title': title

        }
        list_of_dicts.append(channel_data)
        # pprint(snippet)
        # pprint(stats)
        # print(channel_id)
        # pprint(elem)
    pprint(list_of_dicts)
    return list_of_dicts


def main():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_key = "AIzaSyDLT1w9RuCvXpeY3i6CRiM7gPD4Vf_0VEI"
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "client_secret_1080473066558-rh5ihim77tc3qbpvparpjnts926tuk3t.apps.googleusercontent.com.json"

    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id="UCGIY_O-8vW4rfX98KlMkvRg"
    )

    # request = youtube.subscriptions().list(
    #    part="snippet,contentDetails",
    #    id="UC_x5XG1OV2P6uZZ5FSM9Ttw"
    # )

    response = request.execute()
    flattenAndParseSearchResponse(response)


if __name__ == "__main__":
    main()
