""" This file contains post database cleaning function for the single movie data dataframe. """

import pandas as pd
import pytz

import data_cleaning_functions as dcf
import numpy as np


def run_post_db_data_clean():
    movie_data_df = pd.read_pickle("movie_data_df_post_db.pkl")

    print(f"The length of the movie_data_df is {len(movie_data_df)}.")
    movie_data_df.drop_duplicates(subset="imdbID", inplace=True)
    print(f"After dropping duplicates, the length of the movie_data_df is {len(movie_data_df)}.")
    movie_df_length = len(movie_data_df)
    movie_data_df["Released"].fillna(movie_data_df["US_release_date"], inplace=True)
    movie_data_df["Released"].fillna(movie_data_df["GB_release_date"], inplace=True)
    movie_data_df.dropna(subset=["Released"], inplace=True)
    print(f"{movie_df_length - len(movie_data_df)} movies with null values in Released were dropped from the dataset.")
    movie_df_length = len(movie_data_df)
    movie_data_df.dropna(subset=["Director"], inplace=True)
    print(f"{movie_df_length - len(movie_data_df)} movies with null values in Director were dropped from the dataset.")
    movie_df_length = len(movie_data_df)
    movie_data_df.dropna(subset=["Plot"], inplace=True)
    print(f"{movie_df_length - len(movie_data_df)} movies with null values in Plot were dropped from the dataset.")
    movie_df_length = len(movie_data_df)
    movie_data_df.dropna(subset=["movie_cast"], inplace=True)
    print(f"{movie_df_length - len(movie_data_df)} movies "
          f"with null values in movie_cast were dropped from the dataset.")
    movie_data_df["BoxOffice"].replace(to_replace="nan", value=np.nan, inplace=True)
    movie_df_length = len(movie_data_df)
    movie_data_df = movie_data_df[~movie_data_df["Rated"].str.contains("TV", na=False)]
    print(f"{movie_df_length - len(movie_data_df)} movies with Rated as TV were dropped from the dataset.")
    movie_data_df.drop(["Season", "Episode", "seriesID", "totalSeasons"], axis=1, inplace=True)
    print("Cleaning plot...")
    plot_count = 0
    for index, value in movie_data_df["Plot"].items():
        plot_count += 1
        print(f"Currently on plot {plot_count} of {len(movie_data_df)}.")
        movie_data_df.at[index, "Plot"] = dcf.clean_plot(value)
    print("Cleaning rtRating...")
    rt_rating_count = 0
    for index, value in movie_data_df["rtRating"].items():
        rt_rating_count += 1
        print(f"Currently on rtRating {rt_rating_count} of {len(movie_data_df)}.")
        if value is not None:
            movie_data_df.at[index, "rtRating"] = dcf.clean_rt_rating(str(value))
    print("Cleaning movie_cast...")
    movie_cast_count = 0
    for index, value in movie_data_df["movie_cast"].items():
        movie_cast_count += 1
        print(f"Currently on movie_cast {movie_cast_count} of {len(movie_data_df)}.")
        movie_data_df.at[index, "movie_cast"] = dcf.clean_cast(value)
    print("Cleaning movie_crew...")
    movie_crew_count = 0
    for index, value in movie_data_df["movie_crew"].items():
        movie_crew_count += 1
        print(f"Currently on movie_crew {movie_crew_count} of {len(movie_data_df)}.")
        movie_data_df.at[index, "movie_crew"] = dcf.clean_crew(value)

    # Swap the movie_cast and movie_crew column names (to correct naming error in joining)
    movie_data_df.rename(columns={"movie_cast": "movie_crew_temp", "movie_crew": "Movie_Cast"}, inplace=True)
    movie_data_df.rename(columns={"movie_crew_temp": "Movie_Crew"}, inplace=True)

    movie_data_df.rename(columns={"Actors": "Lead_Actors"}, inplace=True)
    print("Creating Supporting_Actors column...")
    movie_data_df["Supporting_Actors"] = movie_data_df.apply(
        lambda row: dcf.get_supporting_actors(row["Movie_Cast"], row["Lead_Actors"]), axis=1)

    movie_data_df["soundtrack_artists"] = movie_data_df["soundtrack_artists"].map(
        lambda x: dcf.remove_redundant_names(x))
    movie_data_df["Writer"] = movie_data_df["Writer"].map(lambda x: dcf.clean_writers(x))
    movie_data_df["Director"] = movie_data_df["Director"].map(lambda x: dcf.split_director_names(x))
    movie_data_df.to_pickle("movie_data_df_post_db_v2.pkl")


def execute_move_actors():
    movie_data_df = pd.read_pickle("movie_data_df_post_db_v3.pkl")
    movie_data_df.apply(lambda row: dcf.move_actors(row["Lead_Actors"], row["Supporting_Actors"], row["Movie_Cast"]),
                        axis=1)
    movie_data_df.to_pickle("movie_data_df_post_db_v4.pkl")


def execute_normalise_columns():
    """Normalise the text and datetime columns by removing redundant characters and making all letters uppercase, and
       making datetimes timezone aware."""
    movie_data_df = pd.read_pickle("movie_data_df_post_db_v2.pkl")
    actor_data_df = pd.read_pickle("actor_data_df_post_db.pkl")
    soundtrack_credits_df = pd.read_pickle("soundtrack_credits_df_post_db.pkl")
    grammy_awards_df = pd.read_pickle("grammy_awards_df_post_db.pkl")
    gg_awards_df = pd.read_pickle("gg_awards_df_post_db.pkl")
    oscar_awards_df = pd.read_pickle("oscar_awards_df_post_db.pkl")

    movie_data_df_str_cols_to_normalise = ["Title", "Rated", "Type", "Awards"]
    for col in movie_data_df_str_cols_to_normalise:
        movie_data_df[col] = movie_data_df[col].map(lambda x: dcf.normalise_string_names(x))

    movie_data_df_list_cols_to_normalise = ["Genre", "Director", "Plot", "Writer", "Lead_Actors", "Alternative_Titles",
                                            "Keyword_List", "Language", "Production", "Movie_Cast", "Movie_Crew",
                                            "soundtrack_songs", "soundtrack_artists", "Supporting_Actors"]
    for col in movie_data_df_list_cols_to_normalise:
        movie_data_df[col] = movie_data_df[col].map(lambda x: dcf.normalise_list_names(x))

    actor_data_df_str_cols_to_normalise = ["actor"]
    for col in actor_data_df_str_cols_to_normalise:
        actor_data_df[col] = actor_data_df[col].map(lambda x: dcf.normalise_string_names(x))

    actor_data_df_list_cols_to_normalise = ["Movie_Credits"]
    for col in actor_data_df_list_cols_to_normalise:
        actor_data_df[col] = actor_data_df[col].map(lambda x: dcf.normalise_list_names(x))

    soundtrack_credits_df_str_cols_to_normalise = ["song_title", "artist_name"]
    for col in soundtrack_credits_df_str_cols_to_normalise:
        soundtrack_credits_df[col] = soundtrack_credits_df[col].map(lambda x: dcf.normalise_string_names(x))

    grammy_df_str_cols_to_normalise = ["ceremony", "category", "nominee", "artist", "workers"]
    for col in grammy_df_str_cols_to_normalise:
        grammy_awards_df[col] = grammy_awards_df[col].map(lambda x: dcf.normalise_string_names(x))

    gg_df_str_cols_to_normalise = ["category", "nominee", "film"]
    for col in gg_df_str_cols_to_normalise:
        gg_awards_df[col] = gg_awards_df[col].map(lambda x: dcf.normalise_string_names(x))

    oscar_df_str_cols_to_normalise = ["category", "name", "film"]
    for col in oscar_df_str_cols_to_normalise:
        oscar_awards_df[col] = oscar_awards_df[col].map(lambda x: dcf.normalise_string_names(x))

    gg_awards_df["year_award"] = pd.to_datetime(gg_awards_df["year_award"], utc=True)
    oscar_awards_df["year_ceremony"] = pd.to_datetime(oscar_awards_df["year_ceremony"], utc=True)
    grammy_awards_df["awards_year"] = pd.to_datetime(grammy_awards_df["awards_year"], utc=True)
    movie_data_df["Released"] = movie_data_df["Released"].apply(lambda d: d.replace(tzinfo=pytz.utc))

    movie_data_df.to_pickle("movie_data_df_post_db_v3.pkl")
    actor_data_df.to_pickle("actor_data_df_post_db_v2.pkl")
    soundtrack_credits_df.to_pickle("soundtrack_credits_df_post_db_v2.pkl")
    grammy_awards_df.to_pickle("grammy_awards_df_post_db_v2.pkl")
    gg_awards_df.to_pickle("gg_awards_df_post_db_v2.pkl")
    oscar_awards_df.to_pickle("oscar_awards_df_post_db_v2.pkl")
