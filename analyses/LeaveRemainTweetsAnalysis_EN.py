import pandas as pd
import plotly.graph_objects as go
import pymongo
from bson.code import Code
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
                                var value = 0;
                                var value = this.salient_words[idx].c;
                                emit(key, value);
                           }
                        };
                  """)

    reducer = Code("""
                    function(key, value) {
                         return Array.sum(value);
                      };
                   """)

    # MAP-REDUCE INVOCATIONS
    tweetsDB.map_reduce(mapper, reducer, out={"replace": "leave_word_counts"},
                        query={"salient_words": {"$exists": True}, "language": "en", "t_stance": "leave"})

    tweetsDB.map_reduce(mapper, reducer, out={"replace": "remain_word_counts"},
                        query={"salient_words": {"$exists": True}, "language": "en", "t_stance": "remain"})



def tweetsCorrectionCoefficient():
    leaveCount = tweetsDB.count_documents({"language": "en", "t_stance": "leave"})
    remainCount = tweetsDB.count_documents({"language": "en", "t_stance": "remain"})
    k = remainCount / leaveCount
    return k



def analysis(correctionCoefficient):

    # DATAFRAMES INSTANTIATION

    dfLeave = pd.DataFrame(mydb["leave_word_counts"].find())
    dfLeave.columns = ["word", "leaveCount"]
    dfLeave = dfLeave.sort_values(by='leaveCount', ascending=False).reset_index(drop=True)
    print(dfLeave)

    dfRemain = pd.DataFrame(mydb["remain_word_counts"].find())
    dfRemain.columns = ["word", "remainCount"]
    dfRemain = dfRemain.sort_values(by='remainCount', ascending=False).reset_index(drop=True)
    print(dfRemain)



    # INDIVIDUAL PLOTS LEAVE & REMAIN

    fig = make_subplots(rows=1, cols=2, start_cell="bottom-left",subplot_titles=("LEAVE", "REMAIN"))
    fig.add_trace(go.Bar(x=list(dfLeave["word"][:30]), y=list(dfLeave["leaveCount"][:30]), name="leave", marker_color="rgb(255,0,0)"),
                  row=1, col=1)
    fig.add_trace(go.Bar(x=list(dfRemain["word"][:30]), y=list(dfRemain["remainCount"][:30]), name="remain", marker_color="rgb(0,82,204)"),
                  row=1, col=2)
    fig.update_layout(title="Top 30 most used words for \"leavers\" and \"remainers\"",
                      font=dict(family="Courier New, monospace", size=18, color="#7f7f7f"))
    fig.update_layout(title={'text': "Top 30 most used words for \"leavers\" and \"remainers\"", 'y': 0.98},
                      font=dict(family="Courier New, monospace", size=18, color="#7f7f7f"))
    for i in fig['layout']['annotations']:
        i['font'] = dict(family="Courier New, monospace", size=25, color="#7f7f7f")
    fig.show()



    # AGGREGATED DATAFRAME
    df = pd.merge(dfLeave, dfRemain, on='word')

    # exclude "brexit" word from the aggregated dataframe
    df = df.drop(df[df['word'] == "brexit"].index)

    # AGGREGATED DATAFRAME (WITH WEIGHTED COEFFICIENT)
    df["leaveCount"] = round(df["leaveCount"] * correctionCoefficient, 0)
    df["sum"] = df["leaveCount"] + df["remainCount"]
    df["leave%"] = round((df["leaveCount"] / df["sum"])*100, 2)
    df["remain%"] = round((df["remainCount"] / df["sum"])*100, 2)
    df = df.sort_values(by='sum', ascending=False).reset_index(drop=True)
    print(df)

    # consider only the first 30 words
    df = df[:30]

    # STACKED BAR CHART
    words = list(df["word"])

    fig = go.Figure(data=[
        go.Bar(name='remain', x=words, y=list(df["remainCount"]), text=list(df["remain%"]), textposition="auto", marker_color="rgb(0,82,204)"),
        go.Bar(name='leave', x=words, y=list(df["leaveCount"]), text=list(df["leave%"]), textposition="auto", marker_color="rgb(255,0,0)")
    ])
    fig.update_layout(barmode='stack')
    fig.update_layout(title="Stance distribution of the top 30 words, adjusted to the same number of \"leavers\" and \"remainers\" [excluded \"brexit\"]",
                      font=dict(family="Courier New, monospace", size=18, color="#7f7f7f"))
    fig.show()

    return



if __name__ == '__main__':
    tweetsMapReduce()
    k = tweetsCorrectionCoefficient()
    analysis(k)
