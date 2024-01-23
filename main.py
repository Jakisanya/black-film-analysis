# coding=utf-8

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


import utils
import pandas as pd
import data_collection
import pre_db_data_cleaning
import post_db_data_cleaning
# import psql_database_eng



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # data_collection.collect_data()
    # pre_db_data_cleaning.run_pre_db_data_clean()
    # psql_database_eng.write_dataframes_to_sql_database()

    """
    tmdb_movie_data_list = pd.DataFrame(utils.load_json_data("original_tmdb_movie_data_list_all_2225.json"))
    print(f'length of tmdb movie data list = {len(tmdb_movie_data_list)}')

    actor_data_list = utils.load_json_data("actor_data_list_all_2225.json")
    actor_data = pd.DataFrame(actor_data_list)
    print(f'number of movie credits in actor_data["Movie_Credits"]: {len(actor_data["Movie_Credits"].sum())}')
    """