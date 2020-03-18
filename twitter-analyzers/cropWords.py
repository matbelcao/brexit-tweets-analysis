import collections
import pymongo

#
#
# THIS FILE DELETES THE WORDS IN THE MONGODB TABLES THAT HAVE MORE THAN 25 CHARACTERS
#
# SHOULD BE USED ONLY IF THE MAP-REDUCE PROCESS INDEXING FAILS...
#
#



# MONGODB CONNECTION
myclient = pymongo.MongoClient("mongodb://Admin:Matteo1996@localhost:27017/")
mydb = myclient["brexit"]
usersDB = mydb["users"]
tweetsDB = mydb["tweets"]



def toCounter(tuple):
    c = collections.Counter()
    for x in tuple:
        c[x["w"]] = int(x["c"])
    return c


def toTuple(counter):
    array = []
    words = list(counter)
    for w in words:
        array.append({"w": w, "c": int(counter[w])})
    return array


if __name__ == '__main__':

    # FIX "users" DB

    mydoc = usersDB.find({"language": {"$exists": True}}, {"_id": 0})
    users = list(mydoc)

    for user in users:
        counter = toCounter(user["salient_words"])
        words = list(counter)
        for w in words:
            if(len(w)>=25):
                print("\nOLD --- ", counter)
                del counter[w]
                print("NEW --- ", counter)
                userTweetWords = toTuple(counter)

                # UPDATING DB
                usersDB.update_one({"user_id": user["user_id"]},{"$set": {"salient_words": userTweetWords}})




    # FIX "tweets" DB

    mydoc = tweetsDB.find({"language": {"$exists": True}}, {"_id": 0})
    tweets = list(mydoc)

    for tweet in tweets:
        counter = toCounter(tweet["salient_words"])
        words = list(counter)
        for w in words:
            if(len(w)>=25):
                print("\nOLD --- ", counter)
                del counter[w]
                print("NEW --- ", counter)
                userTweetWords = toTuple(counter)

                # UPDATING DB
                tweetsDB.update_one({"ID": tweet["ID"]}, {"$set": {"salient_words": userTweetWords}})