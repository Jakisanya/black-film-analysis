""" This file contains the data cleaning functions. """

import pandas as pd
import re
import numpy as np
from nltk.corpus import stopwords

# omdb_movie_data_list.json


def get_rt_score(movie_data):
    """Get the Rotten Tomatoes score for a movie from omdb_movie_data dictionary."""
    ratings = movie_data.get("Ratings")
    if ratings is not None:
        for rating in ratings:
            if rating["Source"] == "Rotten Tomatoes":
                return rating["Value"]
    return np.nan


def split_long_string(string):
    """Split a long string of names into a list of names."""
    split_string = string.split(", ")
    return split_string


def clean_runtime(runtime):
    """Clean movie runtime values."""
    runtime = runtime.strip(" min")
    return runtime


def clean_country(country_list):
    """Clean the country values for each movie."""
    country_list = ['USA' if country == 'United States' else country for country in country_list]
    return country_list


def clean_imdb_votes(votes):
    """Clean imdb votes values."""
    votes = re.sub(",", "", votes)
    return votes


def clean_box_office(box_off_val):
    """Clean box office values."""
    if box_off_val is not None:
        box_off_val = re.sub("[$,]", "", box_off_val)
    return box_off_val


def clean_writers(writers):
    """Clean writers string by removing their roles in parentheses after name."""
    writers = re.sub("(.\([a-zA-Z\s]+\))", "", writers)
    return writers


# tmdb_movie_data_list.json

def append_release_dates(movie_data):
    """Extract the nested theatrical release date/s for a movie, append to the first level of the movie_data
    dictionary; return movie_data."""
    if movie_data["Release_Dates"] is not None:
        for release in movie_data["Release_Dates"]:
            country_code = release.get("iso_3166_1")
            for data in release.get("release_dates"):
                if data["type"] == 3:
                    release_date = data.get("release_date")
                    movie_data[f'{country_code}_release_date'] = release_date
    return movie_data


def append_keywords(movie_data):
    """Extract the nested keywords for a movie and append a list to the first level of the movie_data dictionary;
    return movie_data."""
    keyword_list = []
    if (movie_data["Keywords"] is not None) and (len(movie_data["Keywords"]) != 0):
        for keyword in movie_data["Keywords"]:
            keyword = keyword.get("name")
            keyword_list.append(keyword)
    movie_data["Keyword_List"] = keyword_list
    return movie_data


# golden_globe_awards.csv / the_oscar_award.csv

def convert_award_date(year_award):
    """Add the month to the year and convert to datetime object."""
    year_award = pd.to_datetime(year_award + "-04")
    return year_award


# the_grammy_awards.csv

def clean_grammy_workers(workers_string):
    """Clean the workers column of the grammy awards dataframe and return a list of names."""
    workers_string = re.sub("(\w*\s*\w+;)", "", workers_string)  # remove words followed by ";" e.g. "conductor;".
    workers_string = re.sub("&", ",", workers_string)
    workers_string = re.sub("(\weaturing)", ",", workers_string)
    workers_string = re.sub("(\(\w\))", "", workers_string)  # remove "(A)" and "(T)"
    workers_string = re.sub("[()]", ",", workers_string)

    roles = ["produce", "engineer", "direct", "music", "supervise", "supervisor", "master", "mix",
             "compose", "artist", "conduct", "arrange", "songwriter", "compilation", "restoration", "write", "solo",
             "video", "album", "notes", "lyricist"]

    for i in range(0, len(roles)):
        workers_string = re.sub(f"({roles[i]}\w*)", "", workers_string)

    workers_string = re.sub("[^a-zA-Z0-9,.\+\-\*'$äöüÄÖÜßáéíóúñ]", " ", workers_string)
    workers = workers_string.split(",")

    for worker in workers:
        worker = re.sub(",", "", worker).strip()

    return workers


def clean_grammy_artists(artists_string):
    """Clean the artist column of the grammy awards dataframe and return a list of names."""
    artists_string = re.sub("&", ",", artists_string)
    artists_string = re.sub("(\weaturing)", ",", artists_string)
    artists = artists_string.split(",")
    return artists


def clean_plot(plot):
    """Clean the plot text."""
    plot = re.sub("[^a-zA-Z0-9]", " ", plot).lower()  # remove non-alphanumeric characters
    plot = re.sub("[0-9]+", "number", plot)  # convert all digits to the word "number"
    plot = [word for word in plot.split() if word not in stopwords.words()]  # remove stop words
    return plot


def clean_rt_rating(rt_rating):
    """Clean the Rotten Tomatoes ratings."""
    rt_rating = re.sub("%", "", rt_rating)
    return rt_rating


def clean_cast(movie_cast):
    """Remove duplicates from the movie_cast values."""
    actors = []
    for actor in movie_cast:
        if actor not in actors:
            actors.append(actor)
    return actors


def clean_crew(movie_crew):
    """Remove duplicates from the movie_crew values."""
    crew = []
    for member in movie_crew:
        if member not in crew:
            crew.append(member)
    return crew


def get_supporting_actors(movie_cast, lead_actors):
    """Get the supporting actors by comparing the full cast list with the lead actors list."""
    if (movie_cast is not None) and (lead_actors is not None):
        supporting_actors = set(movie_cast).difference(set(lead_actors))
        return list(supporting_actors)
