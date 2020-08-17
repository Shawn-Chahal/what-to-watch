from uri import uri
import numpy as np
from scipy.sparse import csr_matrix
import pymongo

MIN_RATINGS = 1000
SURVEY_LENGTH = 64
NUM_RESULTS = 32

NUM_USERS = 1000
NUM_TOP_MATCH = 50
BIAS = 1.0

myclient = pymongo.MongoClient(uri, retryWrites=False)
mydb = myclient.get_default_database()
mycol_ratings = mydb['ratings']
mycol_movies = mydb['movies']
mydocs_survey = mycol_movies.aggregate([{'$match': {'ratings-count': {'$gte': MIN_RATINGS}}},
                                        {'$sample': {'size': SURVEY_LENGTH}}])
movie_survey_id = []
movie_survey_title = []

for doc in mydocs_survey:
    movie_survey_id.append(doc['_id'])
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

    mydocs_similar_users = mycol_ratings.aggregate([{'$match': {'movieId': {'$in': movie_survey_id}}},
                                                    {'$sample': {'size': NUM_USERS}}])

    if year_max < year_min:
        year_max = year_min

    csr_userid = []
    csr_movieid = []
    csr_rating = []

    for document in mydocs_similar_users:
        csr_userid.extend([document['_id']] * len(document['rating']))
        csr_movieid.extend(document['movieId'])
        csr_rating.extend(document['rating'])

    X = csr_matrix((csr_rating, (csr_userid, csr_movieid)),
                   shape=(max(csr_userid) + 1, max(csr_movieid + movie_survey_id) + 1))

    user_vector = csr_matrix((user_ratings, ([0] * len(movie_survey_id), movie_survey_id)),
                             shape=(1, max(csr_movieid + movie_survey_id) + 1)).toarray()[0]

    match = X.dot(user_vector)
    match_idx = np.argsort(match)[::-1][:NUM_TOP_MATCH]
    match_sum = np.sum(match[match_idx])
    if match_sum == 0:
        match_sum = 1

    match_proba = np.reshape(match[match_idx] / match_sum * NUM_TOP_MATCH, (-1, 1))

    results_nnz = X[match_idx].getnnz(axis=0)
    results_sum = X[match_idx].multiply(match_proba).sum(axis=0)
    results_vector = np.array((results_sum[0] / (results_nnz + BIAS)))[0]
    result_ids = np.argsort(results_vector)[::-1]

    results_count = 0

    percent_match = []
    movie_title = []
    imdb_link = []
    youtube_link = []

    for movie_id in result_ids[:1000]:

        if user_vector[movie_id] == 0:

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
                        youtube_link.append(f'https://www.youtube.com/watch?v={youtube_id}')

                        results_count += 1

                        if results_count == NUM_RESULTS:
                            break

# RESULTS

print('\nYou should check out:\n')

for i in range(results_count):
    print(movie_title[i])
    print(percent_match[i])
    print(imdb_link[i])
    print(youtube_link[i])
    print('\n----------------------------------------\n')
