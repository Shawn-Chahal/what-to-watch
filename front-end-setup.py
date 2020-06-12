from uri import uri
from collections import Counter
import numpy as np
from scipy.sparse import csr_matrix
import pymongo

num_users = 1000
survey_length = 50
num_top_movies = 1000
num_results = 25

num_top_match = 30
bias = 0.3
min_count = 3

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

movies_count = Counter(csr_movieid)
movies_count_keys = list(movies_count.keys())
movies_count_values = list(movies_count.values())

movie_counts = csr_matrix((movies_count_values, ([0] * len(movies_count_keys), movies_count_keys))).toarray()[0]

max_movieid = movie_counts.shape[0]

movie_ids_top = np.argsort(movie_counts)[::-1][:num_top_movies]

movie_survey_id = []
movie_survey_title = []

for movie_id in rng.choice(movie_ids_top, survey_length, replace=False):
    movie_survey_id.append(int(movie_id))
    doc = mycol_movies.find_one({'_id': int(movie_id)})
    movie_survey_title.append(doc['title'])

# USER INPUT STARTS

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

year_min = int(input('\nEnter a lower bound on the year: '))
year_max = int(input('Enter an upper bound on the year: '))

# USER INPUT ENDS

if sum([abs(i) for i in user_ratings]) == 0:
    print('Try again. Please rate at least one movie.')

else:

    if max(movie_survey_id) > max_movieid:
        max_movieid = max(movie_survey_id)

    if year_max < year_min:
        year_max = year_min

    X = csr_matrix((csr_rating, (csr_userid, csr_movieid)), shape=(max(csr_userid) + 1, max_movieid + 1))

    user_vector = csr_matrix((user_ratings, ([0] * len(movie_survey_id), movie_survey_id)),
                             shape=(1, max_movieid + 1)).toarray()[0]

    match = X.dot(user_vector)
    match_idx = np.argsort(match)[::-1][:num_top_match]
    match_sum = np.sum(match[match_idx])

    if match_sum == 0:
        match_sum = 1

    match_proba = np.reshape(match[match_idx] / match_sum * num_top_match, (-1, 1))

    results_nnz = X[match_idx].getnnz(axis=0)
    results_sum = X[match_idx].multiply(match_proba).sum(axis=0)
    results_vector = np.array((results_sum[0] / (results_nnz + bias)))[0]
    result_ids = np.argsort(results_vector)[::-1]

    results_count = 0

    percent_match = []
    movie_title = []
    imdb_link = []
    youtube_link = []

    for movie_id in result_ids[:1000]:

        if user_vector[movie_id] == 0:
            if results_nnz[movie_id] >= min_count:
                doc = mycol_movies.find_one({'_id': int(movie_id)})

                if doc is not None:

                    title = doc['title']
                    youtube_id = doc['youtubeId']
                    imdb_id = doc['imdbId']

                    if title[-1] == ')':
                        title_year = int(title[-5:-1])

                        if year_min <= title_year <= year_max:

                            percent_match.append(
                                f'{results_vector[movie_id] / results_vector[result_ids[0]] * 0.99:.0%} match')
                            movie_title.append(title)
                            imdb_link.append(f'https://www.imdb.com/title/tt{imdb_id}')
                            youtube_link.append(youtube_id)

                            results_count += 1

                            if results_count == num_results:
                                break

# RESULTS

print('\nYou should check out:\n')

for i in range(results_count):
    print(movie_title[i])
    print(percent_match[i])
    print(imdb_link[i])
    print(f'https://www.youtube.com/watch?v={youtube_link[i]}')
    print('\n----------------------------------------\n')
