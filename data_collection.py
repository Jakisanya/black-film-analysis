""" This file contains the data collection function that retrieves data from imdb, tmdb and omdb. """

import imdb_scrapers
import tmdb_api_functions
import omdb_api_functions


def collect_data():
    # imdb_scrapers.run_actor_name_crawler()
    # imdb_scrapers.save_actor_names_and_ids()
    # imdb_scrapers.verify_actor_name_data()
    # tmdb_api_functions.retrieve_actor_and_movie_data_from_tmdb()
    # tmdb_api_functions.save_original_retrieved_tmdb_data()
    # tmdb_api_functions.summarise_original_tmdb_data_retrieval()
    # omdb_api_functions.retrieve_actor_and_movie_data_from_omdb()
    # omdb_api_functions.compare_retrieved_omdb_movie_data_and_return_additional_titles()
    # tmdb_api_functions.retrieve_additional_actor_and_movie_data_from_tmdb()
    # tmdb_api_functions.save_additional_retrieved_tmdb_data()
    # tmdb_api_functions.concatenate_retrieved_tmdb_data()
    # tmdb_api_functions.retrieve_cast_and_crew_data_from_tmdb()
    # imdb_scrapers.run_soundtrack_credits_crawler()
    # imdb_scrapers.save_soundtrack_credits_data()
    grammy_scraper.run_grammy_awards_crawler()
