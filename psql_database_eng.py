""" This file contains the code for the preparation of dataframes to 
    be ported to psql database. and the subsequent psql transformation 
    scripts to be executed using sqlalchemy. """

import utils
import hidden
from sqlalchemy import create_engine
import pandas as pd
import sqlalchemy_jdbcapi


def convert_remaining_datasets_to_dfs():
    # Name & ID data (collected in the initial IMDb scrape). 
    name_id_list = utils.load_json_data("data_files/actor_names_and_ids.json")
    name_id_df = pd.DataFrame(name_id_list)
    name_id_df.to_pickle("name_id_df_pre_db.pkl")

    # Actors with no TMDb profiles/data.
    actor_not_found_list = utils.load_json_data("data_files/actor_not_found_list_all_2225.json")
    actor_na_df = pd.DataFrame(actor_not_found_list)
    actor_na_df.to_pickle("actor_na_df_pre_db.pkl")


conn_string = hidden.secrets["alchemy"]["connection_string"]

# Create an engine.
engine = create_engine(conn_string)


def write_dataframes_to_sql_database():
    """ Write records stored in DataFrames to SQL databases. """
    print("Starting push to database...")
    with engine.begin() as connection:
        complete_movie_data_df = pd.read_pickle("updated_movie_data_df_post_db_v5.pkl")
        print("Creating complete_movie_data database table...")
        complete_movie_data_df.to_sql("complete_movie_data", con=connection, schema="general", if_exists="replace")
        omdb_df = pd.read_pickle("updated_omdb_df_pre_db.pkl")
        print("Creating omdb_df_data database table...")
        omdb_df.to_sql("updated_omdb_df_data", con=connection, schema="general", if_exists="replace")
        tmdb_df = pd.read_pickle("joined_tmdb_df_pre_db.pkl")
        print("Creating tmdb_df_data database table...")
        tmdb_df.to_sql("joined_tmdb_df_data", con=connection, schema="general", if_exists="replace")
        cast_df = pd.read_pickle("additional_cast_df_pre_db.pkl")
        print("Creating cast_df_data database table...")
        cast_df.to_sql("additional_cast_df_data", con=connection, schema="general", if_exists="replace")
        crew_df = pd.read_pickle("additional_crew_df_pre_db.pkl")
        print("Creating crew_df_data database table...")
        crew_df.to_sql("additional_crew_df_data", con=connection, schema="general", if_exists="replace")
        box_office_df = pd.read_pickle("additional_box_office_df_pre_db.pkl")
        print("Creating box_office_df_data database table...")
        box_office_df.to_sql("additional_box_office_df_data", con=connection, schema="general", if_exists="replace")
        soundtrack_df = pd.read_pickle("additional_soundtrack_credits_df_pre_db.pkl")
        print("Creating soundtrack_df_data database table...")
        soundtrack_df.to_sql("additional_soundtrack_df_data", con=connection, schema="general", if_exists="replace")
        grammy_awards_df = pd.read_pickle("grammy_awards_df_pre_db.pkl")
        print("Creating grammy_awards_df_data database table...")
        grammy_awards_df.to_sql("grammy_awards_df_data", con=connection, schema="general", if_exists="replace")
        gg_awards_df = pd.read_pickle("gg_awards_df_pre_db.pkl")
        print("Creating gg_awards_df_data database table...")
        gg_awards_df.to_sql("gg_awards_df_data", con=connection, schema="general", if_exists="replace")
        oscar_awards_df = pd.read_pickle("oscar_awards_df_pre_db.pkl")
        print("Creating oscar_awards_df_data database table...")
        oscar_awards_df.to_sql("oscar_awards_df_data", con=connection, schema="general", if_exists="replace")
        actor_data_df = pd.read_pickle("actor_df_pre_db.pkl")
        print("Creating actor_data_df_data database table...")
        actor_data_df.to_sql("actor_data_df", con=connection, schema="general", if_exists="replace")
        actor_na_df = pd.read_pickle("actor_na_df_pre_db.pkl")
        print("Creating actor_na_df database table...")
        actor_na_df.to_sql("actor_na_df", con=connection, schema="general", if_exists="replace")
        name_id_df = pd.read_pickle("name_id_df_pre_db.pkl")
        print("Creating name_id_df_data database table...")
        name_id_df.to_sql("name_id_df", con=connection, schema="general", if_exists="replace")

def load_sql_tables_as_dataframes():
    """ Load tables stored in SQL database as dataframes. """
    print("Loading movie_data_df database table...")
    movie_data_df = pd.read_sql_table("movie_data", con=engine, schema="public")
    movie_data_df.to_pickle("updated_movie_data_df_post_db.pkl")
    print("Loading actor_data_df database table...")
    actor_data_df = pd.read_sql_table("actor_data", con=engine, schema="public")
    actor_data_df.to_pickle("actor_data_df_post_db.pkl")
    print("Loading grammy_awards_df database table...")
    grammy_awards_df = pd.read_sql_table("grammy_awards_df_data", con=engine, schema="public")
    grammy_awards_df.to_pickle("grammy_awards_df_post_db.pkl")
    print("Loading oscar_awards_df database table...")
    oscar_awards_df = pd.read_sql_table("oscar_awards_df_data", con=engine, schema="public")
    oscar_awards_df.to_pickle("oscar_awards_df_post_db.pkl")
    print("Loading gg_awards_df database table...")
    gg_awards_df = pd.read_sql_table("gg_awards_df_data", con=engine, schema="public")
    gg_awards_df.to_pickle("gg_awards_df_post_db.pkl")
    print("Loading soundtrack_credits_df database table...")
    soundtrack_credits_df = pd.read_sql_table("joined_soundtrack_df_data", con=engine, schema="public")
    soundtrack_credits_df.to_pickle("updated_soundtrack_credits_df_post_db.pkl")
