""" The initial data cleaning script. """

import data_cleaning_functions as dcf
import utils
import pandas as pd
import numpy as np


def clean_omdb_movie_data():
    omdb_movie_data_from_ids = utils.load_json_data("data_files/omdb_movie_data_from_valid_29120_ids.json")
    omdb_movie_data_from_titles = utils.load_json_data("data_files/omdb_movie_data_from_valid_35403_titles.json")
    omdb_movie_data_list = omdb_movie_data_from_ids + omdb_movie_data_from_titles

    for movie in omdb_movie_data_list:
        movie["rtRating"] = dcf.get_rt_score(movie)

    omdb_df = pd.DataFrame(omdb_movie_data_list)
    omdb_df.set_index("imdbID", inplace=True)
    print(f"There are {len(omdb_df.index)} movies in the omdb dataset.")
    omdb_df.drop(labels=["Website", "Ratings", "Response", "Error"], axis=1, inplace=True)
    omdb_df.drop_duplicates(inplace=True)
    print(f"There are {len(omdb_df.index)} movies in the omdb dataset after a single drop duplicates.")
    omdb_df.replace(to_replace="N/A", value=np.nan, inplace=True)
    omdb_df.dropna(subset=["Type", "Title", "imdbRating", "Genre", "Runtime"], inplace=True)
    print(f"There are {len(omdb_df.index)} movies in the dataset after a dropna on a subset of the omdb dataset.")

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
    tmdb_movie_data_list = utils.load_json_data("data_files/concatenated_tmdb_movie_data_list.json")

    # Add relevant key-value pairs to movie_data dictionaries, load into dataframes and store in a list.
    movie_dataframes = []
    for movie_data in tmdb_movie_data_list:
        movie_data = dcf.append_release_dates(dcf.append_keywords(movie_data))
        movie_df = pd.DataFrame.from_dict(movie_data, orient="index")
        movie_df = movie_df.transpose()
        movie_dataframes.append(movie_df)

    tmdb_df = pd.concat(movie_dataframes, axis=0, ignore_index=True)

    # assert len(tmdb_movie_data_list) == len(tmdb_df)
    tmdb_df.drop(["Release_Dates", "Keywords"], axis=1, inplace=True)
    tmdb_df.iloc[:, 5:] = tmdb_df.iloc[:, 5:].apply(pd.to_datetime)
    tmdb_df.to_pickle("tmdb_df_pre_db.pkl")


def clean_cast_crew_data():
    cast_crew_data_list = utils.load_json_data("data_files/cast_crew_tmdb_movie_data_list.json")

    # Flatten cast and crew data into dataframes and append dataframes to lists.
    cast_dfs = []
    crew_dfs = []
    for cc_data in cast_crew_data_list:
        cast_df = pd.json_normalize(cc_data, record_path="Cast", meta=["TMDb_ID"])
        cast_dfs.append(cast_df)
        crew_df = pd.json_normalize(cc_data, record_path="Crew", meta=["TMDb_ID"])
        crew_dfs.append(crew_df)

    cast_df = pd.concat(cast_dfs)
    crew_df = pd.concat(crew_dfs)
    cast_df.set_index("TMDb_ID", inplace=True)
    crew_df.set_index("TMDb_ID", inplace=True)
    cast_df.drop(labels=["adult", "popularity", "profile_path", "cast_id", "credit_id"], axis=1,
                 inplace=True)
    crew_df.drop(labels=["adult", "popularity", "profile_path", "credit_id"], axis=1,
                 inplace=True)

    cast_df.to_pickle("cast_df_pre_db.pkl")
    crew_df.to_pickle("crew_df_pre_db.pkl")


def clean_actor_data():
    actor_data_list = utils.load_json_data("data_files/actor_data_list_all_2225.json")
    actor_df = pd.DataFrame(actor_data_list)
    actor_df["TMDb_ID"] = actor_df["TMDb_ID"].astype("str")
    actor_df["Birthday"] = pd.to_datetime(actor_df["Birthday"])
    actor_df.to_pickle("actor_df_pre_db.pkl")


def clean_soundtrack_credits_data():
    """Explode the soundtrack credits nested json file so that every row corresponds to
       an artist's track credit in a movie."""
    soundtrack_credits_data = utils.load_json_data("soundtrack_credits_data_list.json")

    soundtrack_credits_exploded = []
    for movie_soundtrack_credits in soundtrack_credits_data:
        if movie_soundtrack_credits["credits"] != list():
            artist_credit = {"imdb_movie_ID": movie_soundtrack_credits["imdb_ID"]}
            for credit in movie_soundtrack_credits["credits"]:
                artist_credit["song_title"] = credit["title"]
                if "performers" in credit:
                    if type(credit["performers"]) is list:
                        for performer in credit["performers"]:
                            artist_credit["artist_imdb_id"] = performer["imdb_id"]
                            artist_credit["artist_name"] = performer["name"]
                            artist_credit["Performed by"] = True
                            artist_credit["Written by"] = False
                            artist_credit["Arranged by"] = False
                            soundtrack_credits_exploded.append(artist_credit.copy())
                    if type(credit["performers"]) is str:
                        if "," in credit["performers"]:
                            performers = credit["performers"].split(",")
                            for performer in performers:
                                artist_credit["artist_imdb_id"] = ""
                                artist_credit["artist_name"] = performer
                                artist_credit["Performed by"] = True
                                artist_credit["Written by"] = False
                                artist_credit["Arranged by"] = False
                                soundtrack_credits_exploded.append(artist_credit.copy())
                        if "," not in credit["performers"]:
                            performer = credit["performers"][13:]
                            artist_credit["artist_imdb_id"] = ""
                            artist_credit["artist_name"] = performer
                            artist_credit["Performed by"] = True
                            artist_credit["Written by"] = False
                            artist_credit["Arranged by"] = False
                            soundtrack_credits_exploded.append(artist_credit.copy())
                if "writers" in credit:
                    if type(credit["writers"]) is list:
                        for writer in credit["writers"]:
                            artist_credit["artist_imdb_id"] = writer["imdb_id"]
                            artist_credit["artist_name"] = writer["name"]
                            artist_credit["Performed by"] = False
                            artist_credit["Written by"] = True
                            artist_credit["Arranged by"] = False
                            soundtrack_credits_exploded.append(artist_credit.copy())
                    if type(credit["writers"]) is str:
                        if "," in credit["writers"]:
                            writers = credit["writers"].split(",")
                            for writer in writers:
                                artist_credit["artist_imdb_id"] = ""
                                artist_credit["artist_name"] = writer
                                artist_credit["Performed by"] = False
                                artist_credit["Written by"] = True
                                artist_credit["Arranged by"] = False
                                soundtrack_credits_exploded.append(artist_credit.copy())
                        else:
                            writer = credit["writers"][12:]
                            artist_credit["artist_imdb_id"] = ""
                            artist_credit["artist_name"] = writer
                            artist_credit["Performed by"] = False
                            artist_credit["Written by"] = True
                            artist_credit["Arranged by"] = False
                            soundtrack_credits_exploded.append(artist_credit.copy())
                if "arrangers" in credit:
                    if type(credit["arrangers"]) is list:
                        for arranger in credit["arrangers"]:
                            artist_credit["artist_imdb_id"] = arranger["imdb_id"]
                            artist_credit["artist_name"] = arranger["name"]
                            artist_credit["Performed by"] = False
                            artist_credit["Written by"] = False
                            artist_credit["Arranged by"] = True
                            soundtrack_credits_exploded.append(artist_credit.copy())
                    if type(credit["arrangers"]) is str:
                        if "," in credit["arrangers"]:
                            arrangers = credit["arrangers"].split(",")
                            for arranger in arrangers:
                                artist_credit["artist_imdb_id"] = ""
                                artist_credit["artist_name"] = arranger
                                artist_credit["Performed by"] = False
                                artist_credit["Written by"] = False
                                artist_credit["Arranged by"] = True
                                soundtrack_credits_exploded.append(artist_credit.copy())
                        else:
                            arranger = credit["arrangers"][13:]
                            artist_credit["artist_imdb_id"] = ""
                            artist_credit["artist_name"] = arranger
                            artist_credit["Performed by"] = False
                            artist_credit["Written by"] = False
                            artist_credit["Arranged by"] = True
                            soundtrack_credits_exploded.append(artist_credit.copy())

    soundtrack_df = pd.DataFrame(soundtrack_credits_exploded)
    soundtrack_df.set_index("imdb_movie_ID", inplace=True)
    soundtrack_df.to_pickle("soundtrack_credits_df_pre_db.pkl")


def clean_golden_globe_data():
    gg_awards_df = pd.read_csv("data_files/golden_globe_awards.csv", encoding='utf-8',
                               encoding_errors='ignore')
    gg_awards_df["year_award"] = gg_awards_df["year_award"].astype("str").map(lambda x: dcf.convert_award_date(x))
    gg_awards_df.to_pickle("gg_awards_df_pre_db.pkl")


def clean_grammy_data():
    grammy_awards_df = pd.DataFrame(utils.load_json_data("data_files/annual_grammy_awards.json"))

    def concat_col_lists(row_value):
        new_row_value = []
        for cell_list in row_value:
            if cell_list == list():
                new_row_value += [" "]
            else:
                new_row_value += cell_list
        return new_row_value

    for col in grammy_awards_df.columns:
        grammy_awards_df[col] = grammy_awards_df[col].map(lambda x: concat_col_lists(x))

    grammy_awards_df = grammy_awards_df.explode(['ceremony', 'awards_year', 'category', 'nominee', 'artist', 'workers',
                                                 'winner'])
    grammy_awards_df.to_pickle("grammy_awards_df_pre_db.pkl")


def clean_oscars_data():
    oscar_awards_df = pd.read_csv("data_files/the_oscar_award.csv", encoding='utf-8',
                                  encoding_errors='ignore')
    oscar_awards_df["year_ceremony"] = oscar_awards_df["year_ceremony"].astype("str").map(
        lambda x: dcf.convert_award_date(x))
    oscar_awards_df.to_pickle("oscar_awards_df_pre_db.pkl")


def clean_box_office_data():
    box_office_data_list = utils.load_json_data("box_office_data_list.json")
    box_office_df = pd.DataFrame(box_office_data_list)
    box_office_df.set_index("IMDb_ID", inplace=True)

    box_office_df["Opening_Weekend_Gross"] = box_office_df["Opening_Weekend_Gross"].map(
        lambda x: dcf.clean_box_office(x))
    box_office_df["Worldwide_Gross"] = box_office_df["Worldwide_Gross"].map(lambda x: dcf.clean_box_office(x))
    print(f"There are {len(box_office_df)} entries in the box office dataset.")
    box_office_df.replace(to_replace="", value=np.nan, inplace=True)
    box_office_df.dropna(subset=["Opening_Weekend_Gross", "Worldwide_Gross"], how='all', inplace=True)
    print(f"There are {len(box_office_df)} entries in the box office dataset after a dropna on 2 columns.")
    box_office_df.to_pickle("box_office_df_pre_db.pkl")


def run_pre_db_data_clean():
    print("Entering clean_omdb_movie_data function.")
    clean_omdb_movie_data()
    print("Entering clean_tmdb_movie_data function.")
    clean_tmdb_movie_data()
    print("Entering clean_cast_crew_data function.")
    clean_cast_crew_data()
    print("Entering clean_actor_data function.")
    clean_actor_data()
    print("Entering clean_soundtrack_credits_data function.")
    clean_soundtrack_credits_data()
    print("Entering clean_golden_globe_data function.")
    clean_golden_globe_data()
    print("Entering clean_grammy_data function.")
    clean_grammy_data()
    print("Entering clean_oscars_data function.")
    clean_oscars_data()
    print("Entering clean_box_office_data function.")
    clean_box_office_data()
