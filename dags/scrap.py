from airflow import Dataset
import os
from googleapiclient.discovery import build
import pandas as pd

VIDEO_DATASET_PATH = Dataset(
    f"{os.path.dirname(os.path.abspath(__file__))}/data/video_data.csv"
)
COMMENTS_DATASET_PATH = Dataset(
    f"{os.path.dirname(os.path.abspath(__file__))}/data/comment_data.csv"
)
PP_VIDEO_DATASET_PATH = Dataset(
    f"{os.path.dirname(os.path.abspath(__file__))}/data/pp_video_data.csv"
)
PP_COMMENTS_DATASET_PATH = Dataset(
    f"{os.path.dirname(os.path.abspath(__file__))}/data/pp_comment_data.csv"
)


api_key = "AIzaSyAxs7uzwcGOqJftvR_p9gqZ2nT6KmwtKj4"
channel_ids = [
    "UCoOae5nYA7VqaXzerajD0lg",
    # more channels here
]
playlist_id = "UUoOae5nYA7VqaXzerajD0lg"
api_service_name = "youtube"
api_version = "v3"

youtube = build(api_service_name, api_version, developerKey=api_key)


def get_video_details(youtube, video_ids):

    all_video_info = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics", id=",".join(video_ids[i : i + 50])
        )
        response = request.execute()

        for video in response["items"]:
            stats_to_keep = {
                "snippet": [
                    "channelTitle",
                    "title",
                    "description",
                    "tags",
                    "publishedAt",
                ],
                "statistics": [
                    "viewCount",
                    "likeCount",
                    "favouriteCount",
                    "commentCount",
                ],
                "contentDetails": ["duration", "definition", "caption"],
            }
            video_info = {}
            video_info["video_id"] = video["id"]

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            all_video_info.append(video_info)

    return pd.DataFrame(all_video_info)


def get_video_ids(youtube, playlist_id):

    video_ids = []

    request = youtube.playlistItems().list(
        part="snippet,contentDetails", playlistId=playlist_id, maxResults=50
    )
    response = request.execute()

    for item in response["items"]:
        video_ids.append(item["contentDetails"]["videoId"])

    next_page_token = response.get("nextPageToken")
    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token,
        )
        response = request.execute()

        for item in response["items"]:
            video_ids.append(item["contentDetails"]["videoId"])

        next_page_token = response.get("nextPageToken")

    return video_ids


def get_comments_in_videos(youtube, video_ids):
    all_comments = []

    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies", videoId=video_id
            )
            response = request.execute()

            comments_in_video = [
                comment["snippet"]["topLevelComment"]["snippet"]["textOriginal"]
                for comment in response["items"][0:10]
            ]
            comments_in_video_info = {
                "video_id": video_id,
                "comments": comments_in_video,
            }

            all_comments.append(comments_in_video_info)

        except:
            # When error occurs - most likely because comments are disabled on a video
            print("Could not get comments for video " + video_id)

    return pd.DataFrame(all_comments)


def scrap_main():
    print("Getting video Ids")
    video_ids = get_video_ids(youtube, playlist_id)
    print("Got video Ids")
    print("Getting Video Data")
    video_df = get_video_details(youtube, video_ids)
    print("Got Video Data")
    print("Getting Comment Data")
    comments_df = get_comments_in_videos(youtube, video_ids)
    print("Got Commments Data")
    video_df.to_csv(VIDEO_DATASET_PATH, index=False)
    comments_df.to_csv(COMMENTS_DATASET_PATH, index=False)
    print("Video Data base : ")
    print(video_df.head())
    print("Commments Data base : ")
    print(comments_df.head())


#
# def get_channel_stats(youtube, channel_ids):
#     all_data = []
#
#     request = youtube.channels().list(
#         part="snippet,contentDetails,statistics", id=",".join(channel_ids)
#     )
#     response = request.execute()
#     for item in response["items"]:
#         data = {
#             "channelName": item["snippet"]["title"],
#             "subscribers": item["statistics"]["subscriberCount"],
#             "views": item["statistics"]["viewCount"],
#             "totalVideos": item["statistics"]["videoCount"],
#             "playlistId": item["contentDetails"]["relatedPlaylists"]["uploads"],
#         }
#
#         all_data.append(data)
#
#     return pd.DataFrame(all_data)
scrap_main()
