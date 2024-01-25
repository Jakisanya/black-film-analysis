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
    data_collection.collect_data()
    # pre_db_data_cleaning.run_pre_db_data_clean()
    # psql_database_eng.write_dataframes_to_sql_database()