""" This file contains the TMDb API data retrieval functions. """

import utils
import requests
import pandas as pd
import hidden


def find_actor(imdb_actor_id):
    """Find an actor's record in TMDb using their IMDb ID and extract."""
    base_url = f'https://api.themoviedb.org/3/find/{imdb_actor_id}'
    params = {"api_key": hidden.secrets["tmdb"]["tmdb_api_key"], "external_source": "imdb_id"}
    response = requests.get(base_url, params=params)
    data = response.json()
    actor_info = data["person_results"]
    return actor_info


def get_tmdb_actor_id(actor_info):
    """Extract an actor's TMDb ID from an actor_info JSON object."""
    tmdb_id = actor_info[0].get("id")
    return tmdb_id


def get_actor_gender(actor_info):
    """Extract an actor's gender from an actor_info JSON object."""
    actor_gender = actor_info[0].get("gender")
    return actor_gender


def get_actor_birthday(tmdb_actor_id):
    """Extract an actor's birthday from an actor_info JSON object."""
    base_url = f'https://api.themoviedb.org/3/person/{tmdb_actor_id}'
    params = {"api_key": hidden.secrets["tmdb"]["tmdb_api_key"]}
    response = requests.get(base_url, params=params)
    data = response.json()
    actor_birthday = data.get("birthday")
    return actor_birthday


def get_movie_credits(tmdb_actor_id):
    """Get an actor's movie credits using their TMDb ID."""
    base_url = f'https://api.themoviedb.org/3/person/{tmdb_actor_id}/movie_credits'
    params = {"api_key": hidden.secrets["tmdb"]["tmdb_api_key"]}
    response = requests.get(base_url, params=params)
    movie_credits = response.json()
    return movie_credits


def get_movie_titles(movie_credits):
    """Extract the movie titles from a movie_credits JSON object."""
    movie_titles = []
    for credit in movie_credits["cast"]:
        title = credit.get("original_title")
        movie_titles.append(title)
    assert len(movie_credits["cast"]) == len(movie_titles)
    return movie_titles


def get_tmdb_movie_ids_from_credits(movie_credits):
    """Extract the tmdb movie IDs from movie_credits JSON."""
    tmdb_movie_ids = []
    for credit in movie_credits["cast"]:
        tmdb_movie_id = credit.get("id")
        tmdb_movie_ids.append(tmdb_movie_id)
    assert len(movie_credits["cast"]) == len(tmdb_movie_ids)
    return tmdb_movie_ids


def get_imdb_movie_id(tmdb_movie_id):
    """Get the external ids for a movie and extract the IMDb id."""
    base_url = f'https://api.themoviedb.org/3/movie/{tmdb_movie_id}/external_ids'
    params = {"api_key": hidden.secrets["tmdb"]["tmdb_api_key"]}
    response = requests.get(base_url, params=params)
    id_data = response.json()
    imdb_id = id_data.get("imdb_id")
    return imdb_id


def get_release_dates(tmdb_movie_id):
    """Get the release dates for a movie."""
    base_url = f'https://api.themoviedb.org/3/movie/{tmdb_movie_id}/release_dates'
    params = {"api_key": hidden.secrets["tmdb"]["tmdb_api_key"]}
    response = requests.get(base_url, params=params)
    data = response.json()
    release_dates = data.get("results")
    return release_dates


def get_cast_and_crew(tmdb_movie_id):
    """Get the cast and crew for a movie."""
    base_url = f'https://api.themoviedb.org/3/movie/{tmdb_movie_id}/credits'
    params = {"api_key": hidden.secrets["tmdb"]["tmdb_api_key"]}
    response = requests.get(base_url, params=params)
    data = response.json()
    cast = data.get("cast")
    crew = data.get("crew")
    return cast, crew


def get_movie_budget(tmdb_movie_id):
    """Get the budget for a movie."""
    base_url = f'https://api.themoviedb.org/3/movie/{tmdb_movie_id}'
    params = {"api_key": hidden.secrets["tmdb"]["tmdb_api_key"]}
    response = requests.get(base_url, params=params)
    data = response.json()
    budget = data.get("budget")
    return budget


def get_movie_keywords(tmdb_movie_id):
    """Get the keywords that have been added to a movie."""
    base_url = f'https://api.themoviedb.org/3/movie/{tmdb_movie_id}/keywords'
    params = {"api_key": hidden.secrets["tmdb"]["tmdb_api_key"]}
    response = requests.get(base_url, params=params)
    data = response.json()
    keywords = data.get("keywords")
    return keywords


def get_alt_movie_titles(tmdb_movie_id):
    """Get all the alternative titles for a movie."""
    base_url = f'https://api.themoviedb.org/3/movie/{tmdb_movie_id}/alternative_titles'
    params = {"api_key": hidden.secrets["tmdb"]["tmdb_api_key"]}
    response = requests.get(base_url, params=params)
    title_data = response.json()
    alt_titles = []
    if title_data.get("titles") is not None:
        for title_dict in title_data.get("titles"):
            alt_title = title_dict.get("title")
            alt_titles.append(alt_title)
    return alt_titles


def search_for_movie_get_tmdb_id(title):
    """Search for a movie in TMDb's database and return the TMDb_id."""
    base_url = f'https://api.themoviedb.org/3/search/movie'
    params = {"api_key": hidden.secrets["tmdb"]["tmdb_api_key"], "query": title}
    response = requests.get(base_url, params=params)
    data = response.json()
    if data.get("results") is not None:
        for movie in data.get("results"):
            if movie.get("title").lower() == title.lower():
                tmdb_id = movie.get("id")
                return tmdb_id


# Initialise the empty lists to store actor and movie data.
actor_data_list = []
tmdb_movie_data_list = []
additional_tmdb_movie_data_list = []

# Initialise an extra list to store the actor IDs that returned no data.
actor_not_found_in_tmdb_list = []


def retrieve_actor_and_movie_data_from_tmdb():
    """Retrieve actor and movie information from TMDb."""

    # Load actor name and imdb id data
    actor_names_and_ids = utils.load_json_data("data_files/actor_names_and_ids.json")

    # Query TMBb database using the defined functions and store the returned data in lists of dictionaries.
    count = 0
    for imdb_actor_id in actor_names_and_ids["imdb_ids"]:
        count += 1
        print(f'Current place in list: {count} / {len(actor_names_and_ids)}')

        if len(find_actor(imdb_actor_id)) != 0:
            tmdb_actor_id = get_tmdb_actor_id(find_actor(imdb_actor_id))
            gender = get_actor_gender(find_actor(imdb_actor_id))
            birthday = get_actor_birthday(tmdb_actor_id)
            movie_credits = get_movie_credits(tmdb_actor_id)
            movie_titles = get_movie_titles(movie_credits)
            tmdb_movie_ids = get_tmdb_movie_ids_from_credits(movie_credits)
            actor_data = dict(IMDb_ID=imdb_actor_id, TMDb_ID=tmdb_actor_id, Gender=gender, Birthday=birthday,
                              Movie_Credits=movie_titles)
            actor_data_list.append(actor_data)

            for tmdb_movie_id in tmdb_movie_ids:
                imdb_movie_id = get_imdb_movie_id(tmdb_movie_id)
                alt_titles = get_alt_movie_titles(tmdb_movie_id)
                release_dates = get_release_dates(tmdb_movie_id)
                movie_keywords = get_movie_keywords(tmdb_movie_id)
                budget = get_movie_budget(tmdb_movie_id)
                tmdb_movie_data = dict(IMDb_ID=imdb_movie_id, TMDb_ID=tmdb_movie_id, Alternative_Titles=alt_titles,
                                       Release_Dates=release_dates, Keywords=movie_keywords, Budget=budget)
                if tmdb_movie_data not in tmdb_movie_data_list:
                    tmdb_movie_data_list.append(tmdb_movie_data)
        else:
            actor_not_found_in_tmdb_list.append(imdb_actor_id)

    utils.save_data_as_json("original_tmdb_movie_data_list.json", tmdb_movie_data_list)
    utils.save_data_as_json("actor_data_list.json", actor_data_list)
    utils.save_data_as_json("actor_not_found.json", actor_not_found_in_tmdb_list)


def save_original_retrieved_tmdb_data():
    """Save the retrieved TMDb data as json files."""
    utils.save_data_as_json("actor_data_list.json", actor_data_list)
    utils.save_data_as_json("original_tmdb_movie_data_list.json", tmdb_movie_data_list)
    utils.save_data_as_json("actor_not_found_list.json", actor_not_found_in_tmdb_list)


def summarise_original_tmdb_data_retrieval():
    """Display the tmdb data retrieval statistics."""
    original_actor_data_list = utils.load_json_data("actor_data_list.json")
    print(f"Tmdb data for {len(original_actor_data_list)} actors were successfully retrieved.\n")

    # Check how many searches returned no data.
    original_actor_not_found_in_tmdb_list = utils.load_json_data("actor_not_found_in_tmdb_list.json")
    print(
        f"{len(original_actor_not_found_in_tmdb_list)} actors were not found in tmdb; no data was retrieved for them.")


def retrieve_additional_actor_and_movie_data_from_tmdb():
    """Retrieve additional actor and movie information from TMDb using the additional tmdb movie data."""
    additional_titles_list = utils.load_json_data("data_files/additional_titles_list.json")

    count = 0
    for title in additional_titles_list["Titles"]:
        count += 1
        print(f'Currently on title {count} / {len(additional_titles_list["Titles"])}')
        tmdb_movie_id = search_for_movie_get_tmdb_id(title)
        imdb_movie_id = get_imdb_movie_id(tmdb_movie_id)
        alt_titles = get_alt_movie_titles(tmdb_movie_id)
        release_dates = get_release_dates(tmdb_movie_id)
        movie_keywords = get_movie_keywords(tmdb_movie_id)
        budget = get_movie_budget(tmdb_movie_id)
        tmdb_movie_data = dict(IMDb_ID=imdb_movie_id, TMDb_ID=tmdb_movie_id, Alternative_Titles=alt_titles,
                               Release_Dates=release_dates, Keywords=movie_keywords, Budget=budget)
        additional_tmdb_movie_data_list.append(tmdb_movie_data)


def save_additional_retrieved_tmdb_data():
    utils.save_data_as_json("data_files/additional_tmdb_movie_data_list.json", additional_tmdb_movie_data_list)


def concatenate_retrieved_tmdb_data():
    """Concat the original and additional tmdb movie data datasets."""
    original_dataset = utils.load_json_data("data_files/original_tmdb_movie_data_list_all_2225.json")
    additional_dataset = utils.load_json_data("data_files/additional_tmdb_movie_data_list.json")
    concatenated_tmdb_movie_data_list = original_dataset + additional_dataset
    print(f'Original dataset size: {len(original_dataset)}'
          f'\nAdditional dataset size: {len(additional_dataset)}'
          f'\nConcatenated dataset size: {len(concatenated_tmdb_movie_data_list)}')
    utils.save_data_as_json("data_files/concatenated_tmdb_movie_data_list.json", concatenated_tmdb_movie_data_list)


def retrieve_cast_and_crew_data_from_tmdb():
    """ Retrieve cast and crew data from TMDb. """
    cast_crew_tmdb_data_list = []
    new_tmdb_movie_data_list = utils.load_json_data("data_files/concatenated_tmdb_movie_data_list.json")

    count = 0
    for movie in new_tmdb_movie_data_list:
        count += 1
        print(f'Currently on movie {count} / {len(new_tmdb_movie_data_list)}')
        if movie["TMDb_ID"] is not None:
            tmdb_movie_id = movie["TMDb_ID"]
            cast_and_crew = get_cast_and_crew(tmdb_movie_id)
            cast = cast_and_crew[0]
            crew = cast_and_crew[1]
            cast_crew_data = dict(TMDb_ID=tmdb_movie_id, Cast=cast, Crew=crew)
            cast_crew_tmdb_data_list.append(cast_crew_data)

    utils.save_data_as_json("data_files/cast_crew_tmdb_movie_data_list.json", cast_crew_tmdb_data_list)
