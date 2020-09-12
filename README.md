#  Brexit Tweets Analysis

This is an optional project developed during the course *"054306 - UNSTRUCTURED AND STREAMING DATA ENGINEERING"* during my studies at *Politecnico di Milano*

### OVERVIEW

The project work consists into analyzing some tweets about the Brexit topic, starting from some information about the tweets stored in a CSV database available at https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KP4XRP.

Using this information in the pre-proccessing phase to locate a subset of users and their related tweets, for each tweet a python script will extrapolate the most salient words applying filtering and transformations in the middle steps of the elaboration, and will store the results (single-tweet and user-aggregate arrays of tuples <word,count>) in a MongoDB database.

<img src="/readme_src/architecture.png" height="500px" ></img>
<br>
<br>

Finally, with different programs written in python will be analyzed the most frequent used words for the English tweets and plotted several graphs that takes into account different parameters like stance, sentiment, language.

<img src="/readme_src/word_counting_eng.png" height="400px" ></img>
<img src="/readme_src/sentiment_distribution_eng.png" height="400px" ></img>
<br>
<br>

The tweets in the 4 main European languages (IT,FR,DE,ES) will also be analyzed, but in this case the analysis will be limited to describing the most used words for each one.

<img src="/readme_src/word_counting_multi.png" height="400px" ></img>
<br>
