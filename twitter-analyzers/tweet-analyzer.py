import datetime as dt
from itertools import takewhile
import collections
import re
import string
import time
import warnings

import nltk
import pymongo
import tweepy
from nltk.corpus import stopwords

nltk.download('stopwords')

warnings.filterwarnings("ignore")


def load_api():
    ''' Function that loads the twitter API after authorizing the user.   '''

    consumer_key = ''
    consumer_secret = ''
    access_token = ''
    access_token_secret = ''
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # load the twitter API via tweepy
    return tweepy.API(auth)


def tweet_search(api, tweetID):
    err = 1
    banned = False
    while err == 1:
        try:
            tweet = api.get_status(tweetID, tweet_mode="extended")
            text = tweet.full_text  # .encode('UTF-8')  causa problemi di output....
            lang = tweet.lang
            # print('%s,%s' % (tweetID, text))
            err = 0  # END_LOOP
        except tweepy.TweepError as e:
            print(e.reason)
            if (e.api_code == 144) or (e.api_code == 179) or (e.api_code == 34):
                text = ""
                lang = ""
                err = 0  # END_LOOP
            elif e.api_code == 63:
                text = ""
                lang = ""
                err = 0  # END_LOOP
                banned = True
                print("\nUSER BANNED\n")
            else:
                print('exception raised, waiting 15 minutes')
                print('(until:', dt.datetime.now() + dt.timedelta(minutes=15), ')')
                time.sleep(15 * 60)
                # CONTINUE LOOP
    return lang, str(text),banned


def strip_links(text):
    link_regex = re.compile('((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)', re.DOTALL)
    links = re.findall(link_regex, text)
    for link in links:
        text = text.replace(link[0], ', ')
    return text


def strip_all_entities(text):
    entity_prefixes = ['@', '#']
    for separator in string.punctuation:
        if separator not in entity_prefixes:
            text = text.replace(separator, ' ')
    words = []
    for word in re.split(r'\s+|[\'\"’,;“”:.()-]\s*', text):  # text-split()
        word = word.strip()
        if word:
            if word[0] not in entity_prefixes:
                words.append(word)
    return ' '.join(words)


LANGUAGES = ["en", "it", "fr", "de", "es"]


def get_stop_words(lang):
    if (lang == "en"):
        return set(stopwords.words('english'))
    if (lang == "fr"):
        return set(stopwords.words('french'))
    if (lang == "it"):
        return set(stopwords.words('italian'))
    if (lang == "de"):
        return set(stopwords.words('german'))
    if (lang == "es"):
        return set(stopwords.words('spanish'))
    return set(stopwords.words('english'))


WORDS_PER_TWEET = 5


def get_items_upto_count_bounded(dct, n):
    data = dct.most_common()
    if len(data) == 0:
        return []
    if len(dct) < n:
        n = len(dct)
    val = data[n - 1][1]  # get the value of n-1th item
    # Now collect all items whose value is greater than or equal to `val`.
    return list(takewhile(lambda x: x[1] >= val, data))  # tuples


def toTuple(counter):
    array = []
    for word in counter:
        array.append({"w": word[0], "c": int(word[1])})
    return array


def toCounter(tuple):
    c = collections.Counter()
    for x in tuple:
        c[x["w"]] = int(x["c"])
    return c


# MONGODB CONNECTION
myclient = pymongo.MongoClient("mongodb://Admin:Matteo1996@localhost:27017/")
mydb = myclient["brexit"]
usersDB = mydb["users"]
tweetsDB = mydb["tweets"]

# GET the user ID on the database
mydoc = usersDB.find_one({"salient_words": {"$exists": False}}, {"user_id": 1, "_id": 0})

have_user = bool(mydoc)

# ---------------------ITEARZIONE USER(START WHILE)-------------------
while have_user:
    print("\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n")

    userLangCount = collections.Counter()
    userLangCount[""] = 0  # safe initialization
    userWordCount = collections.Counter()
    bannedUser = False

    userId = mydoc["user_id"]
    print("USER = " + mydoc["user_id"])

    # GET the tweets yet analyzed on the database
    mydoc = tweetsDB.find({"user_id": userId, "language": {"$exists": True}})
    tweets = list(mydoc)

    # ITERATION ON TWEETS YET ANALYZED
    for t in tweets:
        print("\n------------------------------------------------------------------------------------\n")

        print("TWEET-ID : ", int(t["ID"]))

        if t["language"] in LANGUAGES:
            try:
                # UPDATE THE USER MOST USED LANGUAGE AND TWEET WORDS
                userLangCount[t["language"]] += 1
                userWordCount = userWordCount + toCounter(t["salient_words"])
            except Exception:
                print("ERROR ", int(t["ID"]))

        print("\nALREADY PROCESSED !")

    # GET the tweets not yet analyzed on the database
    mydoc = tweetsDB.find({"user_id": userId, "language": {"$exists": False}})
    tweets = list(mydoc)

    api = load_api()

    # ITERATION ON TWEET NOT YET ANALYZED
    for t in tweets:
        print("\n------------------------------------------------------------------------------------\n")

        tweetID = int(t["ID"])
        print("TWEET-ID : ", tweetID)

        tweet = tweet_search(api, tweetID)

        tweetLang = tweet[0]
        print("LANG : " + tweetLang)

        tweetText = tweet[1]
        print("TEXT : " + tweetText)

        bannedUser = tweet[2]

        if (tweetLang in LANGUAGES) and (tweetText != "") and not(bannedUser):
            tweetTextFiltered = strip_all_entities(strip_links(tweetText.lower()))
            print("TEXT (filtered): " + tweetTextFiltered)

            tweetWords = tweetTextFiltered.split()
            print("WORDS : ", end="")
            for word in tweetWords:
                print(word + " | ", end="")
            print()

            # removing stop-words
            stop_words = get_stop_words(tweetLang)
            tweetNSWs = [word for word in tweetWords if not ((word in stop_words) or (len(word) >= 25))]
            print("WORDS non-stop : ", end="")
            for word in tweetNSWs:
                print(word + " | ", end="")
            print()

            # salience of the words in the tweet (word,count)
            NSWsCount = collections.Counter(tweetNSWs)
            # print("\n",NSWsCount)

            # keep only a limited number of words
            NSWsCountBounded = get_items_upto_count_bounded(NSWsCount, WORDS_PER_TWEET)
            print("\n", NSWsCountBounded)
            NSWsCountBounded = toTuple(NSWsCountBounded)

            # update of the tweet 'language' and 'salient_words' in the db
            tweetsDB.update_one({"ID": str(tweetID)},
                                {"$set": {"language": tweetLang, "salient_words": NSWsCountBounded}})

            # UPDATE THE USER MOST USED LANGUAGE AND TWEET WORDS
            userLangCount[tweetLang] += 1
            userWordCount = userWordCount + toCounter(NSWsCountBounded)

        else:
            print("\nSKIP TEXT ANALYSIS")

            tweetsDB.update_one({"ID": str(tweetID)}, {"$set": {"language": tweetLang, "salient_words": []}})

    print("\n------------------------------------------------------------------------------------")

    userLang = userLangCount.most_common(1)[0][0]
    if len(get_items_upto_count_bounded(userLangCount, 1)) > 1:
        userLang = "mix"
    print("\n[ user :", userId, "] Primary language : ", userLang)  # en fr it de sp mix(mixed)

    userWordCount = get_items_upto_count_bounded(userWordCount, WORDS_PER_TWEET)
    print("\n[ user :", userId, "] Global word counts : ", userWordCount)
    userTweetWords = toTuple(userWordCount)

    # INSERTION OF THE MOST USER WORDS IN THE "USERS" DATABASE
    usersDB.update_one({"user_id": str(userId)},
                       {"$set": {"language": userLang, "salient_words": userTweetWords}})

    # FIND THE NEXT USER
    mydoc = usersDB.find_one({"salient_words": {"$exists": False}}, {"user_id": 1, "_id": 0})
    have_user = bool(mydoc)

print("\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n")
print("NO MORE USERS.  END PROGRAM")
