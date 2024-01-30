""" This file contains the data preprocessing functions. """


def get_total_movie_awards(names, date, oscar_awards_df, gg_awards_df):
    """Get the total number of movie/acting awards won amongst a group of individuals before the given date."""
    award_count = 0
    if names is not None:
        for name in names:
            award_count += len(oscar_awards_df[(oscar_awards_df["year_ceremony"] < date) &
                                               (oscar_awards_df["name"] == name) &
                                               (oscar_awards_df["winner"])])
            award_count += len(gg_awards_df[(gg_awards_df["year_award"] < date) &
                                            (gg_awards_df["nominee"] == name) &
                                            (gg_awards_df["win"])])
    return award_count


def get_total_music_awards(names, date, grammy_awards_df):
    """Get the total number of music awards won amongst a group of individuals before the given date."""
    award_count = 0
    if names is not None:
        for name in names:
            award_count += len(grammy_awards_df[(grammy_awards_df["awards_year"] < date) &
                                                (grammy_awards_df["artist"] == name) &
                                                (grammy_awards_df["winner"])])
    return award_count


def calculate_black_actor_proportion(actors, actor_data_df):
    """Calculate the proportion of Black actors that make up a movie's cast."""
    count = 0
    if actors is not None:
        for name in actors:
            if actor_data_df["actor"].str.contains(name, regex=False, na=False).any():
                count += 1
        if count != 0:
            proportion = count / len(actors)
            return f'{proportion:.2f}'
        else:
            return 0


def create_word_freq_list(df_column):
    """Create a sorted word frequency list."""
    plot_words = df_column.sum()  # concatenate all plots
    word_counts = plot_words.value_counts()  # get word counts
    return word_counts
