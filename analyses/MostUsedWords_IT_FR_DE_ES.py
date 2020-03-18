import pymongo
import plotly.graph_objects as go
from bson.code import Code
import pandas as pd
from plotly.subplots import make_subplots



# MONGODB CONNECTION
myclient = pymongo.MongoClient("mongodb://Admin:Matteo1996@localhost:27017/")
mydb = myclient["brexit"]
usersDB = mydb["users"]
tweetsDB = mydb["tweets"]


def tweetsMapReduce():

    # MONGODB CODE FOR TWEETS ANALYSIS

    mapper = Code("""
                function() {
                    for (var idx = 0; idx < this.salient_words.length; idx++) {
                        var key = this.salient_words[idx].w;
                        if (this.language == "it"){
                            var value = {
                                it: this.salient_words[idx].c,
                                fr: 0,
                                de: 0,
                                es: 0
                            };
                        }else if (this.language == "fr"){
                            var value = {
                                it: 0,
                                fr: this.salient_words[idx].c,
                                de: 0,
                                es: 0
                            };
                        }else if (this.language == "de"){
                            var value = {
                                it: 0,
                                fr: 0,
                                de: this.salient_words[idx].c,
                                es: 0
                            };
                        }else if (this.language == "es"){
                            var value = {
                                it: 0,
                                fr: 0,
                                de: 0,
                                es: this.salient_words[idx].c
                            };
                        }else{
                            var value = {
                                it: 0,
                                fr: 0,
                                de: 0,
                                es: 0
                            };
                        }
                        emit(key, value);
                    }
                };
            """)


    reducer = Code("""
                    function(key, ObjVals)
                    {
                        reducedVal = {it: 0, fr: 0, de: 0, es: 0};

                        for (var idx = 0; idx < ObjVals.length; idx++)
                        {
                            reducedVal.it += ObjVals[idx].it;
                            reducedVal.fr += ObjVals[idx].fr;
                            reducedVal.de += ObjVals[idx].de;
                            reducedVal.es += ObjVals[idx].es;
                        }

                        return reducedVal;
                    };
                """)

    # MAP-REDUCE INVOCATIONS
    tweetsDB.map_reduce(mapper, reducer, out={"replace": "not_english_word_counts"},
                       query={"salient_words": {"$exists": True}, "$or": [{ "language": "it"}, { "language": "fr"}, { "language": "de"}, { "language": "es"}]})
    return


def queryToDf(cursor):
    word = []
    it = []
    fr = []
    de = []
    es = []

    for doc in cursor:
        word.append(doc["_id"])
        it.append(int(doc['value']['it']))
        fr.append(int(doc['value']['fr']))
        de.append(int(doc['value']['de']))
        es.append(int(doc['value']['es']))

    df = pd.DataFrame()
    df["word"] = word
    df["italian"] = it
    df["french"] = fr
    df["deutsch"] = de
    df["spanish"] = es

    return df



def analysis():

    # GLOBAL DATAFRAMES INSTANTIATION
    cursor = mydb["not_english_word_counts"].find({})
    df = queryToDf(cursor)

    # DATAFRAMES PROJECTIONS
    italianDf = df[["word", "italian"]].sort_values(by='italian', ascending=False).reset_index(drop=True)[:20]
    frenchDf = df[["word", "french"]].sort_values(by='french', ascending=False).reset_index(drop=True)[:20]
    deutschDf = df[["word", "deutsch"]].sort_values(by='deutsch', ascending=False).reset_index(drop=True)[:20]
    spanishDf = df[["word", "spanish"]].sort_values(by='spanish', ascending=False).reset_index(drop=True)[:20]

    # PLOTS
    fig = make_subplots(rows=2, cols=2, start_cell="bottom-left", subplot_titles=("DEUTSCH","SPANISH","ITALIAN","FRENCH"))

    fig.add_trace(go.Bar(x=list(deutschDf["word"]), y=list(deutschDf["deutsch"]), name="de", marker_color="rgb(254,48,0)"),row=1, col=1)
    fig.add_trace(go.Bar(x=list(spanishDf["word"]), y=list(spanishDf["spanish"]), name="es",marker_color="rgb(250,230,95)"), row=1, col=2)
    fig.add_trace(go.Bar(x=list(italianDf["word"]), y=list(italianDf["italian"]), name="it", marker_color="rgb(0,179,0)"),row=2, col=1)
    fig.add_trace(go.Bar(x=list(frenchDf["word"]), y=list(frenchDf["french"]), name="fr", marker_color="rgb(0,31,77)"),row=2, col=2)

    fig.update_layout(title={'text': "For each language, the relative 20 most used words", 'y': 0.98},
                      font=dict(family="Courier New, monospace", size=18, color="#7f7f7f"), showlegend=False)
    for i in fig['layout']['annotations']:
        i['font'] = dict(family="Courier New, monospace", size=20, color="#7f7f7f")
    fig.show()


    # EXCLUDE "BREXIT" WORD FROM THE NEXT ANALYSES
    italianDf = italianDf.drop(italianDf[italianDf['word'] == "brexit"].index)
    frenchDf = frenchDf.drop(frenchDf[frenchDf['word'] == "brexit"].index)
    deutschDf  = deutschDf.drop(deutschDf[deutschDf['word'] == "brexit"].index)
    spanishDf = spanishDf.drop(spanishDf[spanishDf['word'] == "brexit"].index)


    # PLOTS (EXCLUDED "BREXIT" WORD)
    fig = make_subplots(rows=2, cols=2, start_cell="bottom-left", subplot_titles=("DEUTSCH","SPANISH","ITALIAN","FRENCH"))

    fig.add_trace(go.Bar(x=list(deutschDf["word"]), y=list(deutschDf["deutsch"]), name="de", marker_color="rgb(254,48,0)"),row=1, col=1)
    fig.add_trace(go.Bar(x=list(spanishDf["word"]), y=list(spanishDf["spanish"]), name="es",marker_color="rgb(250,230,95)"), row=1, col=2)
    fig.add_trace(go.Bar(x=list(italianDf["word"]), y=list(italianDf["italian"]), name="it", marker_color="rgb(0,179,0)"),row=2, col=1)
    fig.add_trace(go.Bar(x=list(frenchDf["word"]), y=list(frenchDf["french"]), name="fr", marker_color="rgb(0,31,77)"),row=2, col=2)

    fig.update_layout(title={'text': "For each language, the relative 20 most used words [excluded \"brexit\" word]", 'y': 0.98},
                      font=dict(family="Courier New, monospace", size=18, color="#7f7f7f"), showlegend=False)
    for i in fig['layout']['annotations']:
        i['font'] = dict(family="Courier New, monospace", size=20, color="#7f7f7f")
    fig.show()

    return



if __name__ == '__main__':
    tweetsMapReduce()
    analysis()