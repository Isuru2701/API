# NLP modules
import spacy
import nltk


#sub-function modules
from nltk.corpus import stopwords
from nltk.sentiment import SentimentIntensityAnalyzer

# api modules
from flask import Flask, request, jsonify

import pymssql
from collections import Counter
from flask_apscheduler import APScheduler
import time
from datetime import datetime



#if models don't exist, download them
try:
    nlp = spacy.load("en_core_web_lg")
except:
    spacy.cli.download("en_core_web_lg")
    nlp = spacy.load("en_core_web_lg")

try:

    stop = set(stopwords.words("english"))
    sia = SentimentIntensityAnalyzer()

except:
    nltk.download('all')
    stop = set(stopwords.words("english"))
    sia = SentimentIntensityAnalyzer()



# structure first instance of table
# kon = pymssql.connect(kon_str)
# cursor = kon.cursor()
# cursor.execute("Select enduser_id, entry_date, content from user_entries")
#
# old_table = cursor.fetchall()

app = Flask(__name__)

# # init scheduler
# # !!deprecated!!
# scheduler = APScheduler()
#
# iteration = 1
#

# def listen():
#     kon = pymssql.connect(kon_str)
#     cursor = kon.cursor()
#     cursor.execute("Select enduser_id, entry_date, content from user_entries")
#     table = cursor.fetchall()
#
#     global old_table, iteration
#     dates = [row[1] for row in table]
#
#     print(iteration)
#     for row, old_row in zip(table, old_table):
#
#         # means existing row
#         if row[1] in dates:
#             if str(row[2]) != str(old_row[2]):
#                 update(row[0], row[1])
#                 print("updated ", row[0], row[1])
#             else:
#                 print("didn't update ", row[0], row[1])
#
#         # means new row
#         else:
#             update(row[0], row[1])
#             print("updated ", row[0], row[1])
#
#     iteration += 1
#     old_table = table
#
#
# scheduler.add_job(func=listen, trigger='interval', minutes=1, id="listener")
#
# scheduler.init_app(app)
# scheduler.start()

server = "celestelunar.database.windows.net"
database = "Lunar"
username = "isuru"
password = "#Spagetthi"

kon = pymssql.connect(server=server, user=username, password=password, database=database)



last_call = datetime.now()
last_exception = None

@app.route("/")
def home():
    flag = True
    try:
        kon = pymssql.connect(server=server, user=username, password=password, database=database)
        cursor = kon.cursor()

    except:
        flag = False

    finally:
        kon.close()

    return jsonify(
        {
            'App': 'Twilight for Celeste',
            'Author': 'Isuru Yahampath (https://github.com/Isuru2701)',
            'Version': '1.0.0 - beta',
            'NLP modules used': 'spacy | nltk.corpus | nltk.sentiment',
            'Last Call': last_call,
            'Last Exception': last_exception,
            'DB CONN': flag
        }
    )


def clean(text):
    text = [word.lower() for word in text.split() if word.lower() not in stop]
    return " ".join(text)


def sentiment(text, user_id, date):
    if str(text) is None:
        return {

            'user_id': user_id,
            'date': date,
            'sentiment': 0
        }
    else:
        return {

            'user_id': user_id,
            'date': date,
            'content': str(text),
            'sentiment': sia.polarity_scores(clean(str(text)))['compound']
        }


# update the triggers and the sentiment fields
# tagged by db when an entry changes or updates
@app.route("/execute")
def update():

    user_id = request.args.get('user')
    date = request.args.get('date')

    kon = pymssql.connect(server=server, user=username, password=password, database=database)
    cursor = kon.cursor()

    params = (user_id, date)
    cursor.execute("Select content from user_entries where enduser_id= %d AND entry_date = %s ", params)
    row = cursor.fetchone()
    score = sentiment(row[0], user_id, date)

    # Delete any pre-existing ones cuz this is called if the content is changed
    if score['sentiment']:
        cursor.execute("Delete from user_score where enduser_id= %d and entry_date= %s", params)
        kon.commit()
        params = (score['user_id'], score['sentiment'], score['date'])
        cursor.execute("insert into user_score values(%d,%s,%d)",params)
        kon.commit()

    # Negatives, if any
    seed = ['depression', 'physical abuse', 'addiction', 'loneliness', 'loss', 'stress', 'injury', 'trauma',
            'self-harm', 'death', 'grievance', 'isolation', 'denial' 'aimless', 'illness', 'anxiety', 'insecurity']

    negative_possibility = tag(seed, row, user_id, date, 'triggers')

    params = (user_id, date)
    cursor.execute("Delete from user_triggers where enduser_id= %d and entry_date= %s", params)
    kon.commit()

    values = (user_id, negative_possibility['possibility'], date)

    if values[1] is not None:
        
        cursor.execute("Insert into user_triggers values(%d, %d, %s)", values)
        kon.commit()

    # Positives, if any
    seed = ['happy', 'love', 'cared for', 'friends', 'self-love', 'secure', 'recognized', 'healthy', 'exercise']
    positive_possibility = tag(seed, row, user_id, date, 'comforts')

    params = (user_id, date)
    cursor.execute("Delete from user_comforts where enduser_id= %d and entry_date= %s", params)
    kon.commit()

    values = (user_id, positive_possibility['possibility'], date)


    if values[1] is not None:
        cursor.execute("Insert into user_comforts values(%d, %d, %s)", values)
        kon.commit()

    last_call = datetime.now()

    kon.close()

    return jsonify(
        {
            'user_id': user_id,
            'date': date,
            'sentiment': score['sentiment'],
            'negative_possibility': negative_possibility['possibility'],
            'positive_possibility': positive_possibility['possibility'],
            'last_call': last_call

        }
    )


# https://aclanthology.org/L16-1590.pdf
# https://www.researchgate.net/publication/362586153_Natural_Language_Processing_for_Mental_Disorders_An_Overview

def tag(core, row, user_id, date, table):
    doc = nlp(str(row))
    kon = pymssql.connect(server=server, user=username, password=password, database=database)
    cursor = kon.cursor()

    similar_words = []
    for cause in core:
        for word in doc:
            if word.has_vector and word.is_lower and word.is_alpha and (
                    word.pos_ in ['ADJ', 'ADV', 'NOUN', 'VERB']):
                if nlp(cause).similarity(nlp(str(word.text))) > 0.6:
                    similar_words.append((cause, word.text, nlp(cause).similarity(word)))

    try:
        first_elements = [t[0] for t in similar_words]
        element_counts = Counter(first_elements)
        cursor.execute(f"select trigger_id from {table} where trigger_name = %d",
                       element_counts.most_common(1)[0][0])

        most_common_element = cursor.fetchone()
        if most_common_element is not None:
            return {
                'user': user_id,
                'date': date,
                'content': str(row),
                'triggers': similar_words,
                'possibility': int(most_common_element[0])
            }
        else:
            return {
                'user': user_id,
                'date': date,
                'content': str(row),
                'triggers': similar_words,
                'possibility': None
            }


    except IndexError:
        return {
            'user': user_id,
            'date': date,
            'content': str(row),
            'triggers': similar_words,
            'possibility': None
        }

    except:
        return None

    finally:
        kon.close()


if __name__ == '__main__':
    app.run(debug=True, port=777)
