import seaborn as sns
import pandas as pd
import os
from airflow import Dataset
import matplotlib.ticker as ticker
import matplotlib.pyplot as plt
import nltk

nltk.download("stopwords")
nltk.download("punkt")
from nltk.corpus import stopwords
from wordcloud import WordCloud

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
PLOTS_PATH = f"{os.path.dirname(os.path.abspath(__file__))}/plots"


def plots_main():
    print("Saving Plots")
    if not os.path.exists(PLOTS_PATH):
        os.makedirs(PLOTS_PATH)

    video_df = pd.read_csv(PP_VIDEO_DATASET_PATH)

    ax = sns.barplot(
        x="title",
        y="viewCount",
        data=video_df.sort_values("viewCount", ascending=False).iloc[0:9],
    )
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, pos: "{:,.0f}".format(x / 1000) + "K")
    )
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_PATH, "top_9_videos_descending.png"))
    plt.clf()

    ax = sns.barplot(
        x="title",
        y="viewCount",
        data=video_df.sort_values("viewCount", ascending=True).iloc[0:9],
    )
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, pos: "{:,.0f}".format(x / 1000) + "K")
    )
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_PATH, "top_9_videos_ascending.png"))
    plt.clf()

    sns.violinplot(x=video_df["channelTitle"], y=video_df["viewCount"])
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_PATH, "violin_plot_channel_vs_viewCount.png"))
    plt.clf()

    fig, ax = plt.subplots(1, 2)
    sns.scatterplot(data=video_df, x="commentCount", y="viewCount", ax=ax[0])
    sns.scatterplot(data=video_df, x="likeCount", y="viewCount", ax=ax[1])
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_PATH, "scatter_plots_comment_like_vs_viewCount.png"))
    plt.clf()

    sns.histplot(data=video_df, x="durationSecs", bins=30)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_PATH, "histogram_durationSecs.png"))
    plt.clf()

    stop_words = set(stopwords.words("english"))
    video_df["title_no_stopwords"] = video_df["title"].apply(
        lambda x: [item for item in str(x).split() if item not in stop_words]
    )

    all_words = list([a for b in video_df["title_no_stopwords"].tolist() for a in b])
    all_words_str = " ".join(all_words)

    def plot_cloud(wordcloud):
        plt.figure(figsize=(30, 20))
        plt.imshow(wordcloud)
        plt.axis("off")

    wordcloud = WordCloud(
        width=2000,
        height=1000,
        random_state=1,
        background_color="black",
        colormap="viridis",
        collocations=False,
    ).generate(all_words_str)

    plot_cloud(wordcloud)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_PATH, "wordcloud_video_titles.png"))
    plt.clf()

    day_df = pd.DataFrame(video_df["pushblishDayName"].value_counts())
    weekdays = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    day_df = day_df.reindex(weekdays)
    ax = day_df.reset_index().plot.bar(x="index", y="pushblishDayName", rot=0)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_PATH, "day_of_week_distribution.png"))
    plt.clf()
    print("Saved plots")


if __name__ == "__main__":
    plots_main()
