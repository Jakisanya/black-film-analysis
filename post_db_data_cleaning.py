""" This file contains post database cleaning function for the single movie data dataframe. """

import pandas as pd
import data_cleaning_functions as dcf
import numpy as np


def run_post_db_data_clean():
    movie_data_df = pd.read_pickle("movie_data_df_post_db.pkl")

    movie_data_df.drop_duplicates(subset="imdbID", inplace=True)
    movie_data_df["Released"].fillna(movie_data_df["US_release_date"], inplace=True)
    movie_data_df["Released"].fillna(movie_data_df["GB_release_date"], inplace=True)
    movie_data_df.dropna(subset=["Released"], inplace=True)
    movie_data_df.drop(labels=["US_release_date", "GB_release_date"], axis=1, inplace=True)
    movie_data_df.dropna(subset=["Director"], inplace=True)
    movie_data_df.dropna(subset=["Plot"], inplace=True)
    movie_data_df.dropna(subset=["movie_cast"], inplace=True)
    movie_data_df["BoxOffice"].replace(to_replace="nan", value=np.nan, inplace=True)
    movie_data_df = movie_data_df[~movie_data_df["Rated"].str.contains("TV", na=False)]
    movie_data_df["Plot"] = movie_data_df["Plot"].astype(str).map(lambda x: dcf.clean_plot(x))
    movie_data_df["rtRating"] = movie_data_df["rtRating"].astype(str).map(lambda x: dcf.clean_rt_rating(x))
    movie_data_df["movie_cast"] = movie_data_df["movie_cast"].map(lambda x: dcf.clean_cast(x))
    movie_data_df["movie_crew"] = movie_data_df["movie_crew"].map(lambda x: dcf.clean_crew(x))

    # Swap the movie_cast and movie_crew column names (to correct naming error in joining)
    movie_data_df.rename(columns={"movie_cast": "movie_crew_temp", "movie_crew": "Movie_Cast"}, inplace=True)
    movie_data_df.rename(columns={"movie_crew_temp": "Movie_Crew"}, inplace=True)

    movie_data_df.rename(columns={"Actors": "Lead_Actors"}, inplace=True)
    movie_data_df["Supporting_Actors"] = movie_data_df.apply(
        lambda row: dcf.get_supporting_actors(row["Movie_Cast"], row["Lead_Actors"]), axis=1)

    movie_data_df.to_pickle("movie_data_df_post_db_v2.pkl")
