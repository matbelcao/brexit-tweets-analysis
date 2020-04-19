import pandas as pd
import plotly.graph_objects as go
import pymongo
from bson.code import Code
from plotly.subplots import make_subplots


# MONGODB CONNECTION
myclient = pymongo.MongoClient("mongodb://Admin:Password@localhost:27017/")
mydb = myclient["brexit"]
usersDB = mydb["users"]
tweetsDB = mydb["tweets"]


def usersMapReduce():

    # MONGODB CODE FOR TWEETS ANALYSIS

    mapper = Code("""
                function() {
                    for (var idx = 0; idx < this.salient_words.length; idx++) {
                        var key = this.salient_words[idx].w;
                        if (this.sentiment == "positive"){
                            var value = {
                                positive: this.salient_words[idx].c,
                                neutral: 0,
                                negative: 0,
                                mixed: 0
                            };
                        }else if (this.sentiment == "neutral"){
                            var value = {
                                positive: 0,
                                neutral: this.salient_words[idx].c,
                                negative: 0,
                                mixed:0
                            };
                        }else if (this.sentiment == "negative"){
                            var value = {
                                positive: 0,
                                neutral: 0,
                                negative: this.salient_words[idx].c,
                                mixed: 0
                            };
                        }else if (this.sentiment == "mixed"){
                            var value = {
                                positive: 0,
                                neutral: 0,
                                negative: 0,
                                mixed: this.salient_words[idx].c
                            };
                        }else{
                            var value = {
                                positive: 0,
                                neutral: 0,
                                negative: 0,
                                mixed: 0
                            };
                        }
                        emit(key, value);
                    }
                };
            """)


    reducer = Code("""
                    function(key, ObjVals)
                    {
                        reducedVal = {positive: 0, neutral: 0, negative: 0, mixed: 0};

                        for (var idx = 0; idx < ObjVals.length; idx++)
                        {
                            reducedVal.positive += ObjVals[idx].positive;
                            reducedVal.neutral += ObjVals[idx].neutral;
                            reducedVal.negative += ObjVals[idx].negative;
                            reducedVal.mixed += ObjVals[idx].mixed;
                        }

                        return reducedVal;
                    };
                """)

    # MAP-REDUCE INVOCATIONS
    usersDB.map_reduce(mapper, reducer, out={"replace": "leave_sentiment_word_counts"},
                       query={"salient_words": {"$exists": True}, "language": "en", "stance": "leave"})

    usersDB.map_reduce(mapper, reducer, out={"replace": "remain_sentiment_word_counts"},
                       query={"salient_words": {"$exists": True}, "language": "en", "stance": "remain"})
    return


def usersCorrectionCoefficient():
    leaveCount = usersDB.count_documents({"language": "en", "stance": "leave"})
    remainCount = usersDB.count_documents({"language": "en", "stance": "remain"})
    k = remainCount / leaveCount
    return k


def queryToDf(cursor):
    word = []
    positive = []
    neutral = []
    negative = []
    mixed = []

    for doc in cursor:
        word.append(doc["_id"])
        positive.append(int(doc['value']['positive']))
        neutral.append(int(doc['value']['neutral']))
        negative.append(int(doc['value']['negative']))
        mixed.append(int(doc['value']['mixed']))

    df = pd.DataFrame()
    df["word"] = word
    df["positive"] = positive
    df["neutral"] = neutral
    df["negative"] = negative
    df["mixed"] = mixed
    df["sum"] = df["positive"] + df["neutral"] + df["negative"] + df["mixed"]

    return df


def analysis(correctionCoefficient):

    # DATAFRAMES INSTANTIATION

    cursor = mydb["remain_sentiment_word_counts"].find({})
    dfRemain = queryToDf(cursor)
    dfRemain = dfRemain.sort_values(by='sum', ascending=False).reset_index(drop=True)
    print(dfRemain)


    cursor = mydb["leave_sentiment_word_counts"].find({})
    dfLeave = queryToDf(cursor)
    dfLeave = dfLeave.sort_values(by='sum', ascending=False).reset_index(drop=True)
    print(dfLeave)


    # INDIVIDUAL PLOTS LEAVE & REMAIN by sentiment

    fig = make_subplots(rows=1, cols=2, start_cell="bottom-left",subplot_titles=("LEAVE", "REMAIN"))

    fig.add_trace(go.Bar(x=list(dfLeave["word"][:15]), y=list(dfLeave["positive"][:15]), name="positive", marker_color="rgb(255,153,153)"),
                  row=1, col=1)
    fig.add_trace(go.Bar(x=list(dfLeave["word"][:15]), y=list(dfLeave["neutral"][:15]), name="neutral", marker_color="rgb(255,0,0)"),
                  row=1, col=1)
    fig.add_trace(go.Bar(x=list(dfLeave["word"][:15]), y=list(dfLeave["negative"][:15]), name="negative", marker_color="rgb(153,0,0)"),
                  row=1, col=1)
    fig.add_trace(go.Bar(x=list(dfLeave["word"][:15]), y=list(dfLeave["mixed"][:15]), name="mixed",marker_color="rgb(246,142,63)"),
                  row=1, col=1)

    fig.add_trace(go.Bar(x=list(dfRemain["word"][:15]), y=list(dfRemain["positive"][:15]), name="positive", marker_color="rgb(128,179,255)"),
                  row=1, col=2)
    fig.add_trace(go.Bar(x=list(dfRemain["word"][:15]), y=list(dfRemain["neutral"][:15]), name="neutral", marker_color="rgb(0,82,204)"),
                  row=1, col=2)
    fig.add_trace(go.Bar(x=list(dfRemain["word"][:15]), y=list(dfRemain["negative"][:15]), name="negative", marker_color="rgb(0,31,77)"),
                  row=1, col=2)
    fig.add_trace(go.Bar(x=list(dfRemain["word"][:15]), y=list(dfRemain["mixed"][:15]), name="mixed", marker_color="rgb(56,215,188)"),
                  row=1, col=2)

    fig.update_layout(title={'text': "Top 15 most used words for \"leavers\" and \"remainers\"", 'y': 0.98},
                      font=dict(family="Courier New, monospace", size=18, color="#7f7f7f"))

    for i in fig['layout']['annotations']:
        i['font'] = dict(family="Courier New, monospace", size=25, color="#7f7f7f")

    fig.show()



    # EXCLUDE "BREXIT" WORD FROM THE NEXT ANALYSES
    dfLeave = dfLeave.drop(dfLeave[dfLeave['word'] == "brexit"].index)
    dfRemain = dfRemain.drop(dfRemain[dfRemain['word'] == "brexit"].index)


    # SUB-DATAFRAMES INSTANTIATION

    LeavePositiveDf = dfLeave[["word", "positive"]].sort_values(by='positive', ascending=False).reset_index(drop=True)[:15]
    LeaveNeutralDf = dfLeave[["word", "neutral"]].sort_values(by='neutral', ascending=False).reset_index(drop=True)[:15]
    LeaveNegativeDf = dfLeave[["word", "negative"]].sort_values(by='negative', ascending=False).reset_index(drop=True)[:15]
    LeaveMixedDf = dfLeave[["word", "mixed"]].sort_values(by='mixed', ascending=False).reset_index(drop=True)[:15]

    RemainPositiveDf = dfRemain[["word", "positive"]].sort_values(by='positive', ascending=False).reset_index(drop=True)[:15]
    RemainNeutralDf = dfRemain[["word", "neutral"]].sort_values(by='neutral', ascending=False).reset_index(drop=True)[:15]
    RemainNegativeDf = dfRemain[["word", "negative"]].sort_values(by='negative', ascending=False).reset_index(drop=True)[:15]
    RemainMixedDf = dfRemain[["word", "mixed"]].sort_values(by='mixed', ascending=False).reset_index(drop=True)[:15]


    # INDIVIDUAL PLOTS LEAVE & REMAIN by sentiment

    fig = make_subplots(rows=2, cols=4, start_cell="bottom-left", subplot_titles=("", "", "", "", "POSITIVE", "NEUTRAL", "NEGATIVE", "MIXED"), shared_yaxes = True)

    fig.add_trace(go.Bar(x=list(LeavePositiveDf["word"]), y=list(LeavePositiveDf["positive"]), name="positive", marker_color="rgb(255,153,153)"),row=2, col=1)
    fig.add_trace(go.Bar(x=list(LeaveNeutralDf["word"]), y=list(LeaveNeutralDf["neutral"]), name="neutral", marker_color="rgb(255,0,0)"),row=2, col=2)
    fig.add_trace(go.Bar(x=list(LeaveNegativeDf["word"]), y=list(LeaveNegativeDf["negative"]), name="negative", marker_color="rgb(153,0,0)"),row=2, col=3)
    fig.add_trace(go.Bar(x=list(LeaveMixedDf["word"]), y=list(LeaveMixedDf["mixed"]), name="mixed",marker_color="rgb(246,142,63)"), row=2, col=4)
    fig.update_yaxes(title_text="LEAVE", row=2, col=1)

    fig.add_trace(go.Bar(x=list(RemainPositiveDf["word"]), y=list(RemainPositiveDf["positive"]), name="positive", marker_color="rgb(128,179,255)"),row=1, col=1)
    fig.add_trace(go.Bar(x=list(RemainNeutralDf["word"]), y=list(RemainNeutralDf["neutral"]), name="neutral", marker_color="rgb(0,82,204)"),row=1, col=2)
    fig.add_trace(go.Bar(x=list(RemainNegativeDf["word"]), y=list(RemainNegativeDf["negative"]), name="negative", marker_color="rgb(0,31,77)"),row=1, col=3)
    fig.add_trace(go.Bar(x=list(RemainMixedDf["word"]), y=list(RemainMixedDf["mixed"]), name="mixed",marker_color="rgb(56,215,188)"), row=1, col=4)
    fig.update_yaxes(title_text="REMAIN", row=1, col=1)

    fig.update_layout(title={'text': "For the stances \"leave\" and \"remain\", the relative 15 most used words for each sentiment [excluded \"brexit\" word]", 'y': 0.98},
                      font=dict(family="Courier New, monospace", size=18, color="#7f7f7f"), showlegend=False)

    for i in fig['layout']['annotations']:
        i['font'] = dict(family="Courier New, monospace", size=20, color="#7f7f7f")

    fig.show()



    # AGGREGATED DATAFRAME COMPUTATION

    df = pd.merge(dfLeave,
                  dfRemain,
                  on='word')
    df.columns = ["word", "leave_positive", "leave_neutral", "leave_negative", "leave_mixed", "leave_sum", "remain_positive",
                  "remain_neutral", "remain_negative", "remain_mixed", "remain_sum"]
    df["leave_positive"] = round(df["leave_positive"] * correctionCoefficient, 0)
    df["leave_neutral"] = round(df["leave_neutral"] * correctionCoefficient, 0)
    df["leave_negative"] = round(df["leave_negative"] * correctionCoefficient, 0)
    df["leave_mixed"] = round(df["leave_mixed"] * correctionCoefficient, 0)
    df["leave_sum"] = df["leave_positive"] + df["leave_neutral"] + df["leave_negative"] + df["leave_mixed"]
    df["tot_sum"] = df["leave_sum"] + df["remain_sum"]
    df = df.sort_values(by='tot_sum', ascending=False).reset_index(drop=True)
    print(df)


    # SELECT THE TOP-30 WORDS

    dfTopWords = df[:30]
    print(dfTopWords)


    # COMPUTE THE PERCENTAGES OF THE TOP-30 WORDS
    dfPercentages = pd.DataFrame()
    dfPercentages["leave_positive%"] = round((dfTopWords["leave_positive"] / dfTopWords["tot_sum"])*100, 2)
    dfPercentages["leave_neutral%"] = round((dfTopWords["leave_neutral"] / dfTopWords["tot_sum"]) * 100, 2)
    dfPercentages["leave_negative%"] = round((dfTopWords["leave_negative"] / dfTopWords["tot_sum"]) * 100, 2)
    dfPercentages["leave_mixed%"] = round((dfTopWords["leave_mixed"] / dfTopWords["tot_sum"]) * 100, 2)
    dfPercentages["remain_positive%"] = round((dfTopWords["remain_positive"] / dfTopWords["tot_sum"])*100, 2)
    dfPercentages["remain_neutral%"] = round((dfTopWords["remain_neutral"] / dfTopWords["tot_sum"]) * 100, 2)
    dfPercentages["remain_negative%"] = round((dfTopWords["remain_negative"] / dfTopWords["tot_sum"]) * 100, 2)
    dfPercentages["remain_mixed%"] = round((dfTopWords["remain_mixed"] / dfTopWords["tot_sum"]) * 100, 2)
    print(dfPercentages)


    # STACKED BAR CHART (TOP-30 WORDS BY SENTIMENT)
    words = list(dfTopWords["word"])
    fig = go.Figure(data=[
        go.Bar(name='remain positive', x=words, y=list(dfTopWords["remain_positive"]), text=list(dfPercentages["remain_positive%"]),
               textposition="auto", marker=go.bar.Marker(color="rgb(128,179,255)")),
        go.Bar(name='remain neutral', x=words, y=list(dfTopWords["remain_neutral"]), text=list(dfPercentages["remain_neutral%"]),
               textposition="auto", marker=go.bar.Marker(color="rgb(0,82,204)")),
        go.Bar(name='remain negative', x=words, y=list(dfTopWords["remain_negative"]), text=list(dfPercentages["remain_negative%"]),
               textposition="auto", marker=go.bar.Marker(color="rgb(0,31,77)")),
        go.Bar(name='remain mixed', x=words, y=list(dfTopWords["remain_mixed"]), text=list(dfPercentages["remain_mixed%"]),
               textposition="auto", marker=go.bar.Marker(color="rgb(56,215,188)")),
        go.Bar(name='leave positive', x=words, y=list(dfTopWords["leave_positive"]), text=list(dfPercentages["leave_positive%"]),
               textposition="auto", marker=go.bar.Marker(color="rgb(255,153,153)")),
        go.Bar(name='leave neutral', x=words, y=list(dfTopWords["leave_neutral"]), text=list(dfPercentages["leave_neutral%"]),
               textposition="auto", marker=go.bar.Marker(color="rgb(255,0,0)")),
        go.Bar(name='leave negative', x=words, y=list(dfTopWords["leave_negative"]), text=list(dfPercentages["leave_negative%"]),
               textposition="auto", marker=go.bar.Marker(color="rgb(153,0,0)")),
        go.Bar(name='leave mixed', x=words, y=list(dfTopWords["leave_mixed"]), text=list(dfPercentages["leave_mixed%"]),
               textposition="auto", marker=go.bar.Marker(color="rgb(246,142,63)"))
    ])
    fig.update_layout(barmode='stack')
    fig.update_layout(title="Sentiment distribution of the top 30 words, adjusted to the same number of \"leavers\" and \"remainers\" [excluded \"brexit\"]",
                      font=dict(family="Courier New, monospace", size=18, color="#7f7f7f"))
    fig.show()

    return


if __name__ == '__main__':
    usersMapReduce()
    k = usersCorrectionCoefficient()
    analysis(k)
