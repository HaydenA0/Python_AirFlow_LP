from airflow import Dataset
import os
import pandas as pd
import isodate
from dateutil import parser

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


def preprocess_video():
    video_df = pd.read_csv(VIDEO_DATASET_PATH)
    numeric_cols = ["viewCount", "likeCount", "favouriteCount", "commentCount"]
    video_df[numeric_cols] = video_df[numeric_cols].apply(
        pd.to_numeric, errors="coerce", axis=1
    )
    video_df["publishedAt"] = video_df["publishedAt"].apply(lambda x: parser.parse(x))
    video_df["pushblishDayName"] = video_df["publishedAt"].apply(
        lambda x: x.strftime("%A")
    )
    video_df["durationSecs"] = video_df["duration"].apply(
        lambda x: isodate.parse_duration(x)
    )
    video_df["durationSecs"] = video_df["durationSecs"].astype("timedelta64[s]")
    video_df["tagCount"] = video_df["tags"].apply(lambda x: 0 if x is None else len(x))
    video_df.to_csv(PP_VIDEO_DATASET_PATH, index=False)


def preprocess_main():
    preprocess_video()
