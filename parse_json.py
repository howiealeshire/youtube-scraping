import json
import sys
from datetime import datetime
from pprint import pprint
import csv


def parse(json_obj):
    data = json_obj
    # all that we have to do here is to parse the JSON we have
    user_id = data['entry_data']['ProfilePage'][0]['graphql']['user']['id']

    next_page_bool = \
        data['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']['page_info'][
            'has_next_page']
    edges = data['entry_data']['ProfilePage'][0]['graphql']['user']['edge_felix_video_timeline']['edges']
    item_list = []
    for i in edges:
        url = 'https://www.instagram.com/p/' + i['node']['shortcode']
        video = i['node']['is_video']
        date_posted_timestamp = i['node']['taken_at_timestamp']
        date_posted_human = datetime.fromtimestamp(date_posted_timestamp).strftime("%d/%m/%Y %H:%M:%S")
        like_count = i['node']['edge_liked_by']['count']
        if like_count is None:
            like_count = ''

        comment_count = i['node']['edge_media_to_comment']['count'] if 'edge_media_to_comment' in i[
            'node'].keys() else ''
        captions = ""
        if i['node']['edge_media_to_caption']:
            for i2 in i['node']['edge_media_to_caption']['edges']:
                captions += i2['node']['text'] + "\n"

        if video:
            image_url = i['node']['display_url']
        else:
            image_url = i['node']['thumbnail_resources'][-1]['src']
        item = {'postURL': url, 'isVideo': video, 'date_posted': date_posted_human,
                'timestamp': date_posted_timestamp, 'likeCount': like_count, 'commentCount': comment_count,
                'image_url': image_url,
                'captions': captions[:-1]}
        item_list.append(item)
    if next_page_bool:
        cursor = \
            data['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']['page_info'][
                'end_cursor']
        di = {'id': user_id, 'first': 12, 'after': cursor}
        print(di)
        params = {'query_hash': 'e769aa130647d2354c40ea6a439bfc08', 'variables': json.dumps(di)}
    return item_list


def read_in_csv(file_path):
    with open(file_path, encoding='utf-8') as f:
        a = [{k: v for k, v in row.items()}
             for row in csv.DictReader(f, skipinitialspace=True)]
    return a


def add_bio_to_each(csv_dict_list):
    def get_id_bio_pairs(csv_dict_list):
        id_bio_pairs_dict = {}
        for row in csv_dict_list:
            user_id = row.get('user_id')
            bio = row.get('biography')
            if user_id not in id_bio_pairs_dict.keys() and bio is not None and bio != '':
                id_bio_pairs_dict[user_id] = bio
        return id_bio_pairs_dict

    id_bio_dict = get_id_bio_pairs(csv_dict_list)
    for row in csv_dict_list:
        if row.get('biography') == '':
            user_id = row.get('user_id')
            if user_id is not None:
                obtained_bio = id_bio_dict.get(user_id)
                if obtained_bio:
                    row['biography'] = obtained_bio
                else:
                    row['biography'] = ''

    return csv_dict_list


def main():
    test_file_path = "C:/Users/howie/PycharmProjects/pythonProject/instascraper/9.json"
    test_file = open(test_file_path)
    loaded_json = json.load(test_file)
    json_item_list = parse(loaded_json)
    pprint(json_item_list)


def make_empty_like_counts_zero(csv_rows):
    for row in csv_rows:
        if (row.get('likeCount') == ''):
            row['likeCount'] = '0'
    return csv_rows


def make_top10_posts_list(csv_rows):
    top_users_dict_list = []
    user_id_set = set()
    for row in csv_rows:
        top_user_dict = {
            'comment_count': row.get('commentCount'),
            'like_count': row.get('likeCount'),
            'user_id': row.get('user_id'),
            'username': row.get('username'),
            'postURL': row.get('postURL'),
            'caption': row.get('captions'),
            'image_url': row.get('image_url'),
            'video_url': row.get('videoURL'),
            'total_like_count' : row.get('total_like_count')
        }
        curr_user_id = top_user_dict.get('user_id')
        if (curr_user_id is not None and curr_user_id not in user_id_set):
            user_id_set.add(curr_user_id)
            top_users_dict_list.append(top_user_dict)
    non_zero_list = []
    for elem in top_users_dict_list:
        like_count = elem.get('like_count')
        if like_count != '0':
            non_zero_list.append(elem)
    return non_zero_list


def write_dictlist_to_csv(dict_list, file_name):
    if (len(dict_list) > 0):
        with open(file_name, 'a', encoding='utf8', newline='') as output_file:
            fc = csv.DictWriter(output_file,
                                fieldnames=dict_list[0].keys(),

                                )
            fc.writeheader()
            fc.writerows(dict_list)
    else:
        print("No items to write. Aborting.")
        sys.exit()


def make_top_users_list(csv_rows):
    top_users_dict_list = []
    user_id_set = set()
    for row in csv_rows:
        top_user_dict = {
            'username': row.get('username'),
            'num_followers': row.get('followerCount'),
            'user_id': row.get('user_id'),
            'bio': row.get('biography'),
            'website': row.get('website'),
            'total_like_count': row.get('total_like_count'),
        }
        if (top_user_dict.get('user_id') not in user_id_set):
            user_id_set.add(top_user_dict.get('user_id'))
            top_users_dict_list.append(top_user_dict)
    return top_users_dict_list


def make_follower_counts_non_empty(csv_dict_list):
    def get_id_bio_pairs(csv_dict_list):
        id_bio_pairs_dict = {}
        for row in csv_dict_list:
            user_id = row.get('user_id')
            bio = row.get('followerCount')
            if user_id not in id_bio_pairs_dict.keys() and bio is not None:
                id_bio_pairs_dict[user_id] = bio
        return id_bio_pairs_dict

    id_bio_dict = get_id_bio_pairs(csv_dict_list)
    for row in csv_dict_list:
        if row.get('followerCount') == '':
            user_id = row.get('user_id')
            if user_id:
                x = id_bio_dict[user_id]
                row['followerCount'] = x #id_bio_dict[user_id]
            else:
                row['followerCount'] = '-1'
    return csv_dict_list


def make_websites_non_empty(csv_dict_list):
    def get_id_bio_pairs(csv_dict_list):
        id_bio_pairs_dict = {}
        for row in csv_dict_list:
            user_id = row.get('user_id')
            bio = row.get('website')
            if user_id not in id_bio_pairs_dict.keys():
                id_bio_pairs_dict[user_id] = bio
        return id_bio_pairs_dict

    id_bio_dict = get_id_bio_pairs(csv_dict_list)
    for row in csv_dict_list:
        if row.get('website') == '':
            user_id = row.get('user_id')
            if user_id is not None:
                x = id_bio_dict[user_id]
                if x:
                    row['website'] = x
                else:
                    row['website'] = ''

    return csv_dict_list


def add_total_likes_to_csv_dict_list(csv_dict_list):
    def get_id_bio_pairs(csv_dict_list):
        id_bio_pairs_dict = {}
        for row in csv_dict_list:
            user_id = row.get('user_id')
            id_bio_pairs_dict[user_id] = 0
        return id_bio_pairs_dict

    id_like_count_dict = get_id_bio_pairs(csv_dict_list)
    for elem in csv_dict_list:
        user_id = elem.get('user_id')
        like_count = elem.get('likeCount')
        id_like_count_dict[user_id] += int(like_count)
    for row in csv_dict_list:
        user_id = row.get('user_id')
        like_count = row.get('likeCount')
        total_like_count = id_like_count_dict[user_id]
        row['total_like_count'] = total_like_count
    return csv_dict_list

def get_users(user_array,csv_dict_list):
    filtered_list = []
    for elem in csv_dict_list:
        if elem.get('username') in user_array:
            filtered_list.append(elem)
    return filtered_list

def main2():
    user_accounts = read_top_users_list('top_users_instagram2.txt')
    test_file_path = "C:/Users/howie/PycharmProjects/pythonProject/instascraper/test3.csv"
    csv_dict_list = read_in_csv(test_file_path)
    csv_dict_list = get_users(user_accounts,csv_dict_list)

    csv_dict_list = add_bio_to_each(csv_dict_list)
    csv_dict_list = make_empty_like_counts_zero(csv_dict_list)
    csv_dict_list = make_follower_counts_non_empty(csv_dict_list)
    csv_dict_list = make_websites_non_empty(csv_dict_list)
    csv_dict_list = add_total_likes_to_csv_dict_list(csv_dict_list)

    top_users = make_top_users_list(csv_dict_list)
    top_posts = make_top10_posts_list(csv_dict_list)
    write_dictlist_to_csv(top_users, "data/analysis_instagram_2020-10-13.csv")
    write_dictlist_to_csv(top_posts, "data/top_instagram_2020-10-13.csv")
    # pprint(csv_dict_list)


def read_top_users_list(file_path):
    with open(file_path) as f:
        lines = f.read().splitlines()
    final_list = []
    for line in lines:
        line2 = line.strip()
        final_list.append(line2)
    return final_list


def file_len(fname):
    with open(fname,encoding='utf-8') as f:
        for i, l in enumerate(f):
            pass
    return i + 1

if __name__ == "__main__":
    #test_file_path = "C:/Users/howie/PycharmProjects/pythonProject/instascraper/test.csv"
    #print(file_len(test_file_path))
    #top_users = read_top_users_list('top_users_instagram2.txt')
    #pprint(top_users)
    #print(len(set(top_users)))
    main2()
