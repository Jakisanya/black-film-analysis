""" This file contains the OMDb API data retrieval functions. """

import utils
import pandas as pd
import requests
import hidden
import time
import json

def get_omdb_movie_info_by_id(imdb_movie_id):
    """Get movie information from OMDb's Web API."""
    base_url = 'http://www.omdbapi.com/?' 
    params = {'apikey': hidden.secrets["omdb"]["omdb_api_key"], 'i': imdb_movie_id, 'type': 'movie', 'plot': 'full'}
    response = requests.get(base_url, params=params)
    if response.ok:
        try:
            movie_info = response.json()
            if movie_info is not None:
                return movie_info
            else:
                print(response.text)
        except json.decoder.JSONDecodeError as e:
            print(f"Request successful. Error decoding JSON: {e}")
    else:
        print(f"Request failed with status code: {response.status_code}")

def get_omdb_movie_info_by_title(title):
    """Get movie information from OMDb's Web API."""
    base_url = 'http://www.omdbapi.com/?' 
    params = {'apikey': hidden.secrets["omdb"]["omdb_api_key"], 't': title, 'type': 'movie', 'plot': 'full'}
    response = requests.get(base_url, params=params)
    if response.ok:
        try:
            movie_info = response.json()
            if movie_info is not None:
                return movie_info
            else:
                print(response.text)
        except json.decoder.JSONDecodeError as e:
            print(f"Request successful. Error decoding JSON: {e}")
    else:
        print(f"Request failed with status code: {response.status_code}")


omdb_movie_data_from_ids_list = []
omdb_movie_data_from_titles_list = []


def retrieve_actor_and_movie_data_from_omdb():
    """Use the stored TMDb data (movie IDs and Titles) to retrieve additional data from OMDb.
       First query the database by ID, then by movie Title. Save both sets of data."""
    """
    tmdb_movie_data_list = pd.DataFrame(utils.load_json_data("original_tmdb_movie_data_list_all_2225.json"))
    tmdb_movie_data_list["IMDb_ID"].fillna("", inplace=True)

    by_id_count = 0
    # Query OMBb database by ID
    for imdb_id in tmdb_movie_data_list["IMDb_ID"][27000:]:
        by_id_count += 1
        print(f'By ID : Currently on movie {by_id_count} / 2126')
        if imdb_id is not None:
            omdb_movie_info = get_omdb_movie_info_by_id(imdb_id)
            omdb_movie_data_from_ids_list.append(omdb_movie_info)

    utils.save_data_as_json("omdb_movie_data_from_ids_from_27000_to_29126.json", omdb_movie_data_from_ids_list)
    """

    actor_data_list = utils.load_json_data("data_files/actor_data_list_all_2225.json")
    actor_data = pd.DataFrame(actor_data_list)

    # Query OMBb database by title
    by_title_count = 0
    for title in actor_data["Movie_Credits"].sum()[32500:35433]:
        by_title_count += 1
        print(f'By title : Currently on movie {by_title_count} / 2933')
        if title is not None:
            omdb_movie_info = get_omdb_movie_info_by_title(title)
            omdb_movie_data_from_titles_list.append(omdb_movie_info)

    utils.save_data_as_json("data_files/omdb_movie_data_from_titles_from_32500_to_35433.json", omdb_movie_data_from_titles_list)


additional_titles_list = {"Titles": []}


def compare_retrieved_omdb_movie_data_and_return_additional_titles():
    """Compare the resulting lists from retrieval (by ID and title) and create a dataset
       of additional movie titles."""

    omdb_movie_data_list_1_ids = pd.DataFrame(utils.load_json_data(
        "data_files/omdb_movie_data_from_valid_29120_ids.json"))
    title_list_1 = omdb_movie_data_list_1_ids[["Title"]]

    omdb_movie_data_list_2_titles = pd.DataFrame(utils.load_json_data(
        "data_files/omdb_movie_data_from_valid_35403_titles.json"))
    title_list_2 = omdb_movie_data_list_2_titles[["Title"]]

    print(f'There are {len(title_list_1)} movies in the dataset made using imdb ids and '
          f'{len(title_list_2)} movies in the dataset made using movie titles.')
    
    title_list_3 = title_list_1.merge(title_list_2, indicator=True, how='outer').loc[lambda x: x['_merge'] != 'both']
    print(f'Length of title_list_3 after merge is: {len(title_list_3)}')

    additional_titles = title_list_3[title_list_3["_merge"] == 'right_only']
    additional_titles = additional_titles.drop_duplicates()
    additional_titles.reset_index(inplace=True)

    print(f'There are {len(additional_titles)} additional titles.')
    print(additional_titles.head())

    [additional_titles_list["Titles"].append(title) for title in additional_titles["Title"]]
    print(f'There should be {len(additional_titles)} in additional_titles_list. There are '
          f'{len(additional_titles_list["Titles"])}.')

    utils.save_data_as_json("data_files/additional_titles_list.json", additional_titles_list)
