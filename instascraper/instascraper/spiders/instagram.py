# -*- coding:
# utf-8 -*-
import scrapy
from urllib.parse import urlencode
import json
from datetime import datetime
from pprint import pprint

API = 'YOURAPIKEY'


def get_url(url):
    payload = {'api_key': API, 'url': url}
    proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
    return proxy_url


def get_array_and_delete_from_file(filename):
    f = open(filename, 'r')
    lst = json.load(f)
    arr_arg = lst[0]
    print(len(lst))
    lst.pop(0)
    f.close()
    print(len(lst))
    f = open(filename, 'w')
    json.dump(lst, f)
    f.close()
    return arr_arg


class InstagramSpider(scrapy.Spider):
    name = 'instagram'

    # allowed_domains = ['api.scraperapi.com']
    # custom_settings = {'CONCURRENT_REQUESTS_PER_DOMAIN': 5}

    def start_requests(self):
        path = r'C:\Users\howie\PycharmProjects\pythonProject\instascraper\remaining_args.json'
        user_accounts = ['dava_m',
                         'dhanashree9',
                         'daviddobrik',
                         'dodgers',
                         'merihdemiral',
                         'design',
                         'wejdene.bk',
                         'devpadikkal19',
                         'dixiedamelio',
                         'danishsait',
                         'dababy',
                         'demirose',
                         'doggface208',
                         'desiperkins',
                         'cowboycerrone',
                         'rivadeneirak',
                         'fedeevalverde',
                         'donya',
                         'daisykeech',
                         'mecaigoderisaof',
                         'douglasemhoff',
                         'jojotodynho',
                         'danlabilic',
                         'pontiacmadeddg',
                         'sophiedee',
                         'danawhite',
                         'ratandboa',
                         'inijedar',
                         '1demetozdemir',
                         'dangershewrote',
                         'domelipa',
                         'lilhuddy',
                         'duyguozaslan',
                         'dojacat',
                         'diamondplatnumz',
                         'doritkemsley',
                         'evaluna',
                         'cukurdizi',
                         'bretmanrock',
                         'robkardashianofficial',
                         'andreideiu_',
                         'djokernole',
                         'lesdomakeup',
                         'stevewilldoit',
                         'demetakalin',
                         'd']


        # user_accounts = get_array_and_delete_from_file(path)
        for username in user_accounts:
            url = f'https://www.instagram.com/{username}/?hl=en'
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):

        x = response.xpath("/html/body/script[1]/text()").extract_first()

        # x2 = response.xpath("/html/body/script[12]/text()").extract_first()
        # pprint(x)
        json_string = x.strip().split('= ')[1][:-1]
        # json_string2 = x2.strip().split(',')[1][:-1]
        data = json.loads(json_string)
        # data2 = json.loads(json_string2)
        # all that we have to do here is to parse the JSON we have
        user_id = data['entry_data']['ProfilePage'][0]['graphql']['user']['id']
        username = data['entry_data']['ProfilePage'][0]['graphql']['user']['username']
        biography = data['entry_data']['ProfilePage'][0]['graphql']['user']['biography']
        follower_count = data['entry_data']['ProfilePage'][0]['graphql']['user']['edge_followed_by']['count']
        if follower_count is None or follower_count == '':
            follower_count = '0'
        website = data['entry_data']['ProfilePage'][0]['graphql']['user']['external_url']

        next_page_bool = \
            data['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']['page_info'][
                'has_next_page']
        edges3 = data['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']['edges']
        # edges2 = data2['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']['edges']
        # edges = edges3 + edges2
        edges = edges3
        for i in edges:
            url = 'https://www.instagram.com/p/' + i['node']['shortcode']
            video = i['node']['is_video']
            date_posted_timestamp = i['node']['taken_at_timestamp']
            date_posted_human = datetime.fromtimestamp(date_posted_timestamp).strftime("%d/%m/%Y %H:%M:%S")
            like_count = i['node']['edge_liked_by']['count']
            if like_count is None or like_count == '':
                like_count = '0'
            like_count2 = i['node']['edge_media_preview_like']['count']
            if like_count2 is not None:
                like_count = like_count2

            comment_count = i['node']['edge_media_to_comment']['count'] if 'edge_media_to_comment' in i[
                'node'].keys() else ''
            captions = ""
            if i['node']['edge_media_to_caption']:
                for i2 in i['node']['edge_media_to_caption']['edges']:
                    captions += i2['node']['text'] + "\n"

            if video:
                image_url = ''
                video_url = i['node']['display_url']
            else:
                image_url = i['node']['thumbnail_resources'][-1]['src']
                video_url = ''
            item = {'username': username, 'user_id': user_id, 'postURL': url, 'isVideo': video,
                    'date_posted': date_posted_human,
                    'timestamp': date_posted_timestamp, 'likeCount': like_count, 'commentCount': comment_count,
                    'image_url': image_url,
                    'captions': captions[:-1], 'biography': biography, 'videoURL': video_url,
                    'followerCount': follower_count, 'website': website}
            if video:
                yield scrapy.Request(url, callback=self.get_video, meta={'item': item})
            else:
                item['videoURL'] = ''
                yield item
        if next_page_bool:
            cursor = \
                data['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']['page_info'][
                    'end_cursor']
            di = {'id': user_id, 'first': 12, 'after': cursor}
            print(di)
            params = {'query_hash': 'e769aa130647d2354c40ea6a439bfc08', 'variables': json.dumps(di)}
            url = 'https://www.instagram.com/graphql/query/?' + urlencode(params)
            yield scrapy.Request(url, callback=self.parse_pages, meta={'pages_di': di})

    def parse_pages(self, response):
        di = response.meta['pages_di']
        data = json.loads(response.text)

        with open(str(900) + '.json', 'w') as outfile:
            json.dump(data, outfile)
        # user_id = ''
        # username = ''
        biography = ''
        user_id = data.get('data').get('user')['edge_owner_to_timeline_media'].get('edges')[0]['node']['owner'].get(
            'id')
        username = data.get('data').get('user')['edge_owner_to_timeline_media'].get('edges')[0]['node']['owner'].get(
            'username')
        # biography = ''
        for i in data['data']['user']['edge_owner_to_timeline_media']['edges']:
            video = i['node']['is_video']
            url = 'https://www.instagram.com/p/' + i['node']['shortcode']
            if video:
                image_url = i['node']['display_url']
                video_url = i['node']['video_url']
            else:
                video_url = ''
                image_url = i['node']['thumbnail_resources'][-1]['src']
            date_posted_timestamp = i['node']['taken_at_timestamp']
            captions = ""
            if i['node']['edge_media_to_caption']:
                for i2 in i['node']['edge_media_to_caption']['edges']:
                    captions += i2['node']['text'] + "\n"
            comment_count = i['node']['edge_media_to_comment']['count'] if 'edge_media_to_comment' in i[
                'node'].keys() else ''
            date_posted_human = datetime.fromtimestamp(date_posted_timestamp).strftime("%d/%m/%Y %H:%M:%S")
            like_count = i['node']['edge_media_preview_like']['count'] if "edge_media_preview_like" in i[
                'node'].keys() else ''
            item = {'username': username, 'user_id': user_id, 'postURL': url, 'isVideo': video,
                    'date_posted': date_posted_human,
                    'timestamp': date_posted_timestamp, 'likeCount': like_count, 'commentCount': comment_count,
                    'image_url': image_url,
                    'captions': captions[:-1], 'biography': biography, 'videoURL': video_url, 'followerCount': '',
                    'website': ''}
            yield item
        next_page_bool = data['data']['user']['edge_owner_to_timeline_media']['page_info']['has_next_page']
        if next_page_bool:
            cursor = data['data']['user']['edge_owner_to_timeline_media']['page_info']['end_cursor']
            di['after'] = cursor
            params = {'query_hash': 'e769aa130647d2354c40ea6a439bfc08', 'variables': json.dumps(di)}
            url = 'https://www.instagram.com/graphql/query/?' + urlencode(params)
            yield scrapy.Request(url, callback=self.parse_pages, meta={'pages_di': di})

    def get_video(self, response):
        # only from the first page
        item = response.meta['item']
        video_url = response.xpath('//meta[@property="og:video"]/@content').extract_first()
        item['videoURL'] = video_url
        yield item
