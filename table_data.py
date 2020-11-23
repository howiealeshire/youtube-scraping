from dataclasses import dataclass

table_name_channels: str = 'channels'
table_name_posts: str = 'instagram_post'
table_name_insta_users: str = 'instagram_users'
table_name_searched_insta_users: str = 'instagram_users_searched'
table_name_videos: str = 'videos'
table_name_words_used: str = 'words_to_use_in_search'
table_name_country_codes: str = 'region'
table_name_yt_api_keys: str = 'yt_api_keys'

@dataclass(init=True, repr=True, eq=True, order=False, unsafe_hash=False, frozen=False)
class YTChannel:
    channel_id: str
    title: str
    sub_count: int
    video_count: int
    view_count: int
    country: str = None
    has_been_exported : bool = False


@dataclass(init=True, repr=True, eq=True, order=False, unsafe_hash=False, frozen=False)
class InstaPost:
    comment_count: int
    like_count: int
    user_id: int
    username: str
    postURL: str
    caption : str = None
    video_url: str = None
    total_like_count: int = 0
    has_been_exported : bool = False
    image_url: str = None


@dataclass(init=True, repr=True, eq=True, order=False, unsafe_hash=False, frozen=False)
class InstaUser:
    user_id : int
    username: str
    num_followers: int
    website: str
    bio: str
    total_like_count: int
    has_been_exported : bool = False


@dataclass(init=True, repr=True, eq=True, order=False, unsafe_hash=False, frozen=False)
class InstaUserSearched:
    username: str
    has_been_used: bool = False


@dataclass(init=True, repr=True, eq=True, order=False, unsafe_hash=False, frozen=False)
class YTVideo:
    video_id: str
    channel_id: str
    video_title: str
    description: str
    channel_title: str
    view_count: int
    has_been_exported : bool = False

@dataclass(init=True, repr=True, eq=True, order=False, unsafe_hash=False, frozen=False)
class SearchWord:
    word: str
    search_type: str
    platform: str
    has_been_used: bool

@dataclass(init=True, repr=True, eq=True, order=False, unsafe_hash=False, frozen=False)
class Region:
    region_code: str
    search_type: str
    platform: str
    has_been_used: bool


@dataclass(init=True, repr=True, eq=True, order=False, unsafe_hash=False, frozen=False)
class YtAPIKey:
    api_key: str
    used_today: bool = False




