from uri import uri
from collections import Counter
import os
import pandas as pd
import pymongo

myclient = pymongo.MongoClient(uri)
mydb = myclient.get_default_database()
mycol_ratings = mydb['ratings']
mycol_movies = mydb['movies']

df = pd.read_csv(os.path.join('movielens', 'ratings.csv'))

rating_min = 0.5
rating_max = 5.0

df['rating'] = (df['rating'].to_numpy() - rating_min) / (rating_max - rating_min) * 2 - 1

users_list = list(df['userId'])
users_count = Counter(users_list)

num_users = 20000

df_count = pd.DataFrame.from_dict(users_count, orient='index', columns=['count'])
df_count.sort_values(by=['count'], ascending=False, inplace=True)
user_ids = df_count.index[:num_users]
user_ids = sorted(user_ids)
len_user_ids = len(user_ids)

count = 0

for user_id in user_ids:
    index_list = (df['userId'] == user_id)

    mydict = {'_id': user_id,
              'movieId': list(df['movieId'].loc[index_list]),
              'rating': list(df['rating'].loc[index_list])}

    mycol_ratings.insert_one(mydict)

    count += 1

    if count % int(len_user_ids / 100) == 0:
        print(f'{count / len_user_ids:.0%}')

df_movies = pd.read_csv(os.path.join('movielens', 'movies.csv'), index_col='movieId')
df_youtube = pd.read_csv(os.path.join('movielens', 'ml-youtube.csv'), index_col='movieId')
df_links = pd.read_csv(os.path.join('movielens', 'links.csv'), index_col='movieId',
                       dtype={'imdbId': str, 'tmdbId': str})

for movie_id in df_movies.index:

    title = df_movies['title'].loc[movie_id]
    genres = df_movies['genres'].loc[movie_id]

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

    mydict = {'_id': movie_id,
              'title': title,
              'genres': genres,
              'youtubeId': youtube_id,
              'imdbId': imdb_id,
              'tmdbId': tmdb_id}

    mycol_movies.insert_one(mydict)
