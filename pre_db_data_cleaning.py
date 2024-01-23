""" The initial data cleaning script. """

import data_cleaning_functions as dcf
import utils
import pandas as pd
import numpy as np


def clean_omdb_movie_data():
    omdb_movie_data_from_ids = utils.load_json_data("omdb_movie_data_from_valid_29120_ids.json")
    omdb_movie_data_from_titles = utils.load_json_data("omdb_movie_data_from_valid_35403_titles.json")
    omdb_movie_data_list = omdb_movie_data_from_ids + omdb_movie_data_from_titles

    for movie in omdb_movie_data_list:
        movie["rtRating"] = dcf.get_rt_score(movie)

    omdb_df = pd.DataFrame(omdb_movie_data_list)
    omdb_df.set_index("imdbID", inplace=True)

    omdb_df.drop_duplicates(inplace=True)
    omdb_df.drop(labels=["Awards", "Website", "Ratings", "Response", "Error"], axis=1, inplace=True)
    omdb_df.replace(to_replace="N/A", value=np.nan, inplace=True)
    omdb_df.dropna(subset=["Type"], inplace=True)
    omdb_df.dropna(subset=["Title", "imdbRating", "Genre", "Runtime"], inplace=True)

    omdb_df["Writer"] = omdb_df["Writer"].astype("str").map(lambda x: dcf.clean_writers(x))
    omdb_df["Runtime"] = omdb_df["Runtime"].astype("str").map(lambda x: dcf.clean_runtime(x))
    omdb_df["imdbVotes"] = omdb_df["imdbVotes"].astype("str").map(lambda x: dcf.clean_imdb_votes(x))
    omdb_df["BoxOffice"] = omdb_df["BoxOffice"].astype("str").map(lambda x: dcf.clean_box_office(x))

    # Split the long strings ("ls") in each column into lists.
    ls_cols = ["Genre", "Country", "Writer", "Actors", "Production", "Language"]
    for col in ls_cols:
        omdb_df[col] = omdb_df[col].map(lambda x: dcf.split_long_string(x), na_action='ignore')

    omdb_df["Country"] = omdb_df["Country"].map(lambda x: dcf.clean_country(x), na_action='ignore')
    omdb_df["Released"] = pd.to_datetime(omdb_df["Released"])

    omdb_df.to_pickle("omdb_df_pre_db.pkl")


def clean_tmdb_movie_data():
    tmdb_movie_data_list = utils.load_json_data("concatenated_tmdb_movie_data_list.json")

    # Add relevant key-value pairs to movie_data dictionaries, load into dataframes and store in a list.
    movie_dataframes = []
    for movie_data in tmdb_movie_data_list:
        movie_data = dcf.append_release_dates(dcf.append_keywords(movie_data))
        movie_df = pd.DataFrame.from_dict(movie_data, orient="index")
        movie_df = movie_df.transpose()
        movie_dataframes.append(movie_df)

    tmdb_df = pd.concat(movie_dataframes, axis=0, ignore_index=True)

    # assert len(tmdb_movie_data_list) == len(tmdb_df)

    tmdb_df.iloc[:, 6:] = tmdb_df.iloc[:, 6:].apply(pd.to_datetime)
    tmdb_df.drop(labels=["Release_Dates", "Keywords"], axis=1, inplace=True)

    tmdb_df.to_pickle("tmdb_df_pre_db.pkl")


def clean_cast_crew_data():
    cast_crew_data_list = utils.load_json_data("cast_crew_tmdb_data_list.json")

    # Flatten cast and crew data into dataframes and append dataframes to lists.
    cast_dfs = []
    crew_dfs = []
    for cc_data in cast_crew_data_list:
        cast_df = pd.json_normalize(cc_data, record_path="Cast", meta=["TMDb_ID"])
        cast_dfs.append(cast_df[:15])  # only append the first 15 actors (first billed cast)
        crew_df = pd.json_normalize(cc_data, record_path="Crew", meta=["TMDb_ID"])
        crew_dfs.append(crew_df)

    cast_df = pd.concat(cast_dfs)
    crew_df = pd.concat(crew_dfs)
    cast_df.set_index("TMDb_ID", inplace=True)
    crew_df.set_index("TMDb_ID", inplace=True)
    cast_df.drop(labels=["adult", "popularity", "profile_path", "cast_id", "character", "credit_id"], axis=1,
                 inplace=True)
    crew_df.drop(labels=["adult", "popularity", "profile_path", "known_for_department", "credit_id"], axis=1,
                 inplace=True)

    cast_df.to_pickle("cast_df_pre_db.pkl")
    crew_df.to_pickle("crew_df_pre_db.pkl")


def clean_actor_data():
    actor_data_list = utils.load_json_data("actor_data_list.json")
    actor_df = pd.DataFrame(actor_data_list)
    actor_df["TMDb_ID"] = actor_df["TMDb_ID"].astype("str")
    actor_df["Birthday"] = pd.to_datetime(actor_df["Birthday"])
    actor_df.to_pickle("actor_df_pre_db.pkl")


def clean_soundtrack_credits_data():
    soundtrack_credits_data_list = utils.load_json_data("soundtrack_credits_data_list.json")
    soundtrack_df = pd.DataFrame(soundtrack_credits_data_list)
    soundtrack_df.set_index("IMDb_ID", inplace=True)
    soundtrack_df.to_pickle("soundtrack_credits_df_pre_db.pkl")


def clean_golden_globe_data():
    gg_awards_df = pd.read_csv("golden_globe_awards.csv")
    gg_awards_df["year_award"] = gg_awards_df["year_award"].astype("str").map(lambda x: dcf.convert_award_date(x))
    gg_awards_df.drop(labels=["year_film", "ceremony", "category", "film", "win"], axis=1, inplace=True)
    gg_awards_df.to_pickle("gg_awards_df_pre_db.pkl")


def clean_grammy_data():
    grammy_awards_df = pd.read_csv("the_grammy_awards.csv")
    grammy_awards_df.drop(labels=["year", "title", "updated_at", "category", "nominee", "img", "winner"], axis=1,
                          inplace=True)
    grammy_awards_df["published_at"] = grammy_awards_df["published_at"].map(lambda x: pd.to_datetime(x).date())
    grammy_awards_df["published_at"] = pd.to_datetime(grammy_awards_df["published_at"])
    grammy_awards_df["artist"] = grammy_awards_df["artist"].astype("str").map(lambda x: dcf.clean_grammy_artists(x))
    grammy_awards_df["workers"] = grammy_awards_df["workers"].astype("str").map(lambda x: dcf.clean_grammy_workers(x))
    grammy_awards_df.to_pickle("grammy_awards_df_pre_db.pkl")


def clean_oscars_data():
    oscar_awards_df = pd.read_csv("the_oscar_award.csv")
    oscar_awards_df["year_ceremony"] = oscar_awards_df["year_ceremony"].astype("str").map(
        lambda x: dcf.convert_award_date(x))
    oscar_awards_df["winner"].replace(to_replace=False, value=np.nan, inplace=True)
    oscar_awards_df.dropna(subset=["winner"], inplace=True)
    oscar_awards_df.drop(labels=["year_film", "ceremony", "category", "film", "winner"], axis=1, inplace=True)
    oscar_awards_df.to_pickle("oscar_awards_df_pre_db.pkl")


def clean_box_office_data():
    box_office_data_list = utils.load_json_data("box_office_data_list.json")
    box_office_df = pd.DataFrame(box_office_data_list)
    box_office_df.set_index("IMDb_ID", inplace=True)

    box_office_df["Opening_Weekend_Gross"] = box_office_df["Opening_Weekend_Gross"].map(
        lambda x: dcf.clean_box_office(x))
    box_office_df["Worldwide_Gross"] = box_office_df["Worldwide_Gross"].map(lambda x: dcf.clean_box_office(x))

    box_office_df.replace(to_replace="", value=np.nan, inplace=True)
    box_office_df.dropna(subset=["Opening_Weekend_Gross", "Worldwide_Gross"], how='all', inplace=True)
    box_office_df.to_pickle("box_office_df_pre_db.pkl")


def run_pre_db_data_clean():
    clean_omdb_movie_data()
    clean_tmdb_movie_data()
    clean_cast_crew_data()
    clean_actor_data()
    clean_soundtrack_credits_data()
    clean_golden_globe_data()
    clean_grammy_data()
    clean_oscars_data()
    clean_box_office_data()
