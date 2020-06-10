from uri import uri
from collections import Counter
import numpy as np
from scipy.sparse import csr_matrix
import pymongo

num_users = 1000
survey_length = 24
num_top_movies = 1000
num_results = 12

num_top_match = 30
bias = 1

rng = np.random.default_rng()

myclient = pymongo.MongoClient(uri, retryWrites=False)
mydb = myclient.get_default_database()
mycol_ratings = mydb['ratings']
mycol_movies = mydb['movies']
mydocs_ratings = mycol_ratings.aggregate([{'$sample': {'size': num_users}}])

csr_userid = []
csr_movieid = []
csr_rating = []

for document in mydocs_ratings:
    doc_userid = document['_id']
    doc_movieid = document['movieId']
    doc_rating = document['rating']

    csr_userid.extend([doc_userid] * len(doc_rating))
    csr_movieid.extend(doc_movieid)
    csr_rating.extend(doc_rating)

X = csr_matrix((csr_rating, (csr_userid, csr_movieid)))
max_movieid = X.shape[1]

movies_count = Counter(csr_movieid)
movies_count_keys = list(movies_count.keys())
movies_count_values = list(movies_count.values())

movie_counts = csr_matrix((movies_count_values, ([0] * len(movies_count_keys), movies_count_keys)),
                          shape=(1, max_movieid)).toarray()[0]

movie_ids_top = np.argsort(movie_counts)[::-1][:num_top_movies]

movie_survey_id = []
movie_survey_title = []

for movie_id in rng.choice(movie_ids_top, survey_length, replace=False):
    if movie_id <= max_movieid:
        movie_survey_id.append(int(movie_id))
        doc = mycol_movies.find_one({'_id': int(movie_id)})
        movie_survey_title.append(doc['title'])

user_ratings = []

print('\nDo you like the following movies? y/n\n')
for title in movie_survey_title:
    rating = input(f'{title}: ')
    if rating == 'y':
        user_ratings.append(1)
    elif rating == 'n':
        user_ratings.append(-1)
    else:
        user_ratings.append(0)

user_vector = csr_matrix((user_ratings, ([0] * len(movie_survey_id), movie_survey_id)),
                         shape=(1, max_movieid)).toarray()[0]

match = X.dot(user_vector)
match_idx = np.argsort(match)[::-1][:num_top_match]
match_proba = np.reshape(match[match_idx] / np.sum(match[match_idx]) * num_top_match, (-1, 1))

results_nnz = X[match_idx].getnnz(axis=0)
results_sum = X[match_idx].multiply(match_proba).sum(axis=0)
results_vector = np.array((results_sum[0] / (results_nnz + bias)))[0]
result_ids = np.argsort(results_vector)[::-1]

year_min = int(input('\nEnter a lower bound on the year: '))
year_max = int(input('Enter an upper bound on the year: '))

results_count = 0

print('\nYou should check out:\n')

for movie_id in result_ids[:1000]:

    if user_vector[movie_id] == 0:
        if results_nnz[movie_id] > 1:
            doc = mycol_movies.find_one({'_id': int(movie_id)})

            if doc is not None:

                title = doc['title']
                youtube_id = doc['youtubeId']
                imdb_id = doc['imdbId']

                if title[-1] == ')':
                    title_year = int(title[-5:-1])

                    if year_min <= title_year <= year_max:

                        print(f'Count: {results_nnz[movie_id]:2} |',
                              f'{results_vector[movie_id] / results_vector[result_ids[0]] * 0.99:2.0%} match |',
                              title,
                              f'https://www.imdb.com/title/tt{imdb_id}',
                              f'https://www.youtube.com/watch?v={youtube_id}')

                        results_count += 1

                        if results_count == num_results:
                            break
