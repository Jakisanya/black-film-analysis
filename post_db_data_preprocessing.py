import data_preprocessing_functions as dppf
import pandas as pd


def add_total_award_columns(movie_data_df):
    """ Add the new total award columns to the dataframe, where the number of awards received by an individual/s before
        their movie release data is summed. """
    oscar_awards_df = pd.read_pickle("oscar_awards_df_post_db_v2.pkl")
    grammy_awards_df = pd.read_pickle("grammy_awards_df_post_db_v2.pkl")
    gg_awards_df = pd.read_pickle("gg_awards_df_post_db_v2.pkl")
    movie_data_df["Total_Awards_Lead_Actors"] = movie_data_df.apply(
        lambda row: dppf.get_total_movie_awards(row["Lead_Actors"], row["Released"],
                                                oscar_awards_df, gg_awards_df), axis=1)
    movie_data_df["Total_Awards_Supporting_Actors"] = movie_data_df.apply(
        lambda row: dppf.get_total_movie_awards(row["Supporting_Actors"], row["Released"],
                                                oscar_awards_df, gg_awards_df), axis=1)
    movie_data_df["Total_Awards_Movie_Cast"] = movie_data_df.apply(
        lambda row: dppf.get_total_movie_awards(row["Movie_Cast"], row["Released"],
                                                oscar_awards_df, gg_awards_df), axis=1)
    movie_data_df["Total_Awards_Director"] = movie_data_df.apply(
        lambda row: dppf.get_total_movie_awards(row["Director"], row["Released"],
                                                oscar_awards_df, gg_awards_df), axis=1)
    movie_data_df["Total_Awards_Writer"] = movie_data_df.apply(
        lambda row: dppf.get_total_movie_awards(row["Writer"], row["Released"],
                                                oscar_awards_df, gg_awards_df), axis=1)
    movie_data_df["Total_Awards_Movie_Crew"] = movie_data_df.apply(
        lambda row: dppf.get_total_movie_awards(row["Movie_Crew"], row["Released"],
                                                oscar_awards_df, gg_awards_df), axis=1)
    movie_data_df["Total_Awards_Soundtrack_Credits"] = movie_data_df.apply(
        lambda row: dppf.get_total_music_awards(row["soundtrack_artists"], row["Released"], grammy_awards_df), axis=1)

    return movie_data_df


def add_black_actor_proportion_columns(movie_data_df):
    actor_data_df = pd.read_pickle("actor_data_df_post_db_v2.pkl")
    movie_data_df["Black_Lead_Proportion"] = movie_data_df.apply(
        lambda row: dppf.calculate_black_actor_proportion(row["Lead_Actors"], actor_data_df), axis=1)
    movie_data_df["Black_Support_Proportion"] = movie_data_df.apply(
        lambda row: dppf.calculate_black_actor_proportion(row["Supporting_Actors"], actor_data_df), axis=1)
    movie_data_df["Black_Cast_Proportion"] = movie_data_df.apply(
        lambda row: dppf.calculate_black_actor_proportion(row["Movie_Cast"], actor_data_df), axis=1)

    movie_data_df["Black_Lead_Proportion"] = movie_data_df["Black_Lead_Proportion"].astype(float)
    movie_data_df["Black_Support_Proportion"] = movie_data_df["Black_Support_Proportion"].astype(float)
    movie_data_df["Black_Cast_Proportion"] = movie_data_df["Black_Cast_Proportion"].astype(float)

    return movie_data_df


def run_post_db_data_preprocessing():
    movie_data_df = pd.read_pickle("movie_data_df_post_db_v4.pkl")
    print("Adding total award columns...")
    movie_data_df = add_total_award_columns(movie_data_df)
    print("Adding black actor proportion columns...")
    movie_data_df = add_black_actor_proportion_columns(movie_data_df)
    movie_data_df.to_pickle("movie_data_df_post_db_v5.pkl")

