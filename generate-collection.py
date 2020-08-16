from uri import uri
from collections import Counter
import os
import time
import pandas as pd
import pymongo

MIN_VOTES = 250
UPDATE_FREQUENCY = 30  # seconds

df_ratings = pd.read_csv(os.path.join('movielens', 'ratings.csv'))

print('Preprocessing...')

rating_min = df_ratings['rating'].min()
rating_max = df_ratings['rating'].max()

df_ratings['rating'] = df_ratings['rating'].map(
    lambda x: (x - rating_min) / (rating_max - rating_min) * 2 - 1)

set_users = set(df_ratings['userId'])
counter_users = Counter(df_ratings['userId'].to_list())
top_users = [user_id for user_id in set_users if counter_users[user_id] >= MIN_VOTES]

set_movie_ids = set(df_ratings['movieId'].to_list())
n_movie_ratings = Counter(df_ratings['movieId'].to_list())

df_movies = pd.read_csv(os.path.join('movielens', 'movies.csv'))
df_movies.drop([index for index, movieid in zip(df_movies.index, df_movies['movieId']) if movieid not in set_movie_ids],
               inplace=True)

df_movies['ratings-count'] = df_movies['movieId'].map(n_movie_ratings)
df_youtube = pd.read_csv(os.path.join('movielens', 'ml-youtube.csv'), index_col='movieId')
df_links = pd.read_csv(os.path.join('movielens', 'links.csv'), index_col='movieId',
                       dtype={'imdbId': str, 'tmdbId': str})

myclient = pymongo.MongoClient(uri)
mydb = myclient.get_default_database()
mycol_ratings = mydb['ratings']
mycol_movies = mydb['movies']
'''
print('Creating user ratings documents...')
start_time = time.time()
last_update = time.time()
mydocs_ratings = []
for i, user_id in enumerate(top_users):
    index_list = (df_ratings['userId'] == user_id)

    mydict = {'_id': user_id,
              'movieId': df_ratings['movieId'].loc[index_list].to_list(),
              'rating': df_ratings['rating'].loc[index_list].to_list()}

    mydocs_ratings.append(mydict)

    if time.time() - last_update > UPDATE_FREQUENCY:
        print(f'{i / len(top_users):.1%}')
        last_update = time.time()
'''
print('Creating movie metadata documents...')
mydocs_movies = []
for i in range(df_movies.index.size):

    movie_id = df_movies['movieId'].iloc[i]
    title = df_movies['title'].iloc[i]
    genres = df_movies['genres'].iloc[i]
    ratings_count = df_movies['ratings-count'].iloc[i]

    if movie_id in df_youtube.index:
        youtube_id = df_youtube['youtubeId'].loc[movie_id]
    else:
        youtube_id = 'none'

    if movie_id in df_links.index:
        if df_links['imdbId'].loc[movie_id] == df_links['imdbId'].loc[movie_id]:
            imdb_id = df_links['imdbId'].loc[movie_id]
        else:
            imdb_id = 'none'

        if df_links['tmdbId'].loc[movie_id] == df_links['tmdbId'].loc[movie_id]:
            tmdb_id = df_links['tmdbId'].loc[movie_id]
        else:
            tmdb_id = 'none'
    else:
        imdb_id = 'none'
        tmdb_id = 'none'

    mydict = {'_id': int(movie_id),
              'title': title,
              'genres': genres,
              'youtubeId': youtube_id,
              'imdbId': str(imdb_id),
              'tmdbId': str(tmdb_id),
              'ratings-count': int(ratings_count)}

    mydocs_movies.append(mydict)
'''
print('Uploading ratings to database...')
mycol_ratings.insert_many(mydocs_ratings)
'''
print('Uploading movie metadata to database...')
mycol_movies.insert_many(mydocs_movies)

print('Complete.')
