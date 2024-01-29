""" This file contains post database cleaning function for the single movie data dataframe. """

import pandas as pd
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
    print(f"{movie_df_length - len(movie_data_df)} movies with null values in Released where dropped from the dataset.")
    movie_df_length = len(movie_data_df)
    movie_data_df.dropna(subset=["Director"], inplace=True)
    print(f"{movie_df_length - len(movie_data_df)} movies with null values in Director where dropped from the dataset.")
    movie_df_length = len(movie_data_df)
    movie_data_df.dropna(subset=["Plot"], inplace=True)
    print(f"{movie_df_length - len(movie_data_df)} movies with null values in Plot where dropped from the dataset.")
    movie_df_length = len(movie_data_df)
    movie_data_df.dropna(subset=["movie_cast"], inplace=True)
    print(f"{movie_df_length - len(movie_data_df)} movies "
          f"with null values in movie_cast where dropped from the dataset.")
    movie_data_df["BoxOffice"].replace(to_replace="nan", value=np.nan, inplace=True)
    movie_data_df = movie_data_df[~movie_data_df["Rated"].str.contains("TV", na=False)]
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
        print(f"Currently on rtRating {movie_cast_count} of {len(movie_data_df)}.")
        movie_data_df.at[index, "movie_cast"] = dcf.clean_cast(value)
    print("Cleaning movie_crew...")
    movie_crew_count = 0
    for index, value in movie_data_df["crew_cast"].items():
        movie_crew_count += 1
        print(f"Currently on rtRating {movie_crew_count} of {len(movie_data_df)}.")
        movie_data_df.at[index, "movie_crew"] = dcf.clean_crew(value)

    # Swap the movie_cast and movie_crew column names (to correct naming error in joining)
    movie_data_df.rename(columns={"movie_cast": "movie_crew_temp", "movie_crew": "Movie_Cast"}, inplace=True)
    movie_data_df.rename(columns={"movie_crew_temp": "Movie_Crew"}, inplace=True)

    movie_data_df.rename(columns={"Actors": "Lead_Actors"}, inplace=True)
    print("Creating Supporting_Actors column...")
    movie_data_df["Supporting_Actors"] = movie_data_df.apply(
        lambda row: dcf.get_supporting_actors(row["Movie_Cast"], row["Lead_Actors"]), axis=1)

    movie_data_df.to_pickle("movie_data_df_post_db_v2.pkl")
