# coding=utf-8

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


import utils
import pandas as pd
import data_collection
import pre_db_data_cleaning
import psql_database_eng
import post_db_data_cleaning
import html_parser
import post_db_data_preprocessing

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    html_parser.run_html_parser()
    data_collection.collect_data()
    pre_db_data_cleaning.run_pre_db_data_clean()
    psql_database_eng.convert_remaining_datasets_to_dfs()
    psql_database_eng.write_dataframes_to_sql_database()
    psql_database_eng.load_sql_tables_as_dataframes()
    post_db_data_cleaning.run_post_db_data_clean()
    post_db_data_cleaning.execute_normalise_columns()
    post_db_data_cleaning.execute_move_actors()
    post_db_data_preprocessing.run_post_db_data_preprocessing()
