#  Brexit Tweets Analysis

This is an optional project developed during the course *"054306 - UNSTRUCTURED AND STREAMING DATA ENGINEERING"* during my studies at *Politecnico di Milano*

<br>

### OVERVIEW ( full details in <span style="color:red">report.pdf</span> )



The project work consists into analyzing some tweets about the Brexit topic and plotting some __diagrams__ about the __most frequent words__, taking into account different dimensions like __political stance__, __sentiment__ and __language__.
The original starting CSV data are available at https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KP4XRP.

The tweets data are gathered using a __multithread python script__ in order to exploit a parallel (multi-account) interaction with the __Twitter APIs__. The script extrapolates the most salient words by applying filtering and transformation operations in the middle steps of the elaboration using the __nltk library__, and then stores the results with different granularities (*single-tweet* and *user-aggregate* arrays of tuples <word,count>) in a __MongoDB database__.

<img src="/readme_src/architecture.png" height="500px" ></img>
<br>
<br>

Finally, using several python scripts, are extracted the most frequent used words for the tweets written in English by exploiting a set of __MAP-REDUCE__ queries over the MongoDB repository. The outputs are plots of several graphs that takes into account different parameters like policitcal stance, sentiment and language.

<img src="/readme_src/word_counting_eng.png" height="400px" ></img>
<img src="/readme_src/sentiment_distribution_eng.png" height="400px" ></img>
<br>
<br>

The same kind of analysis is performed for the 4 main European languages (IT,FR,DE,ES), but in this case the output is limited on describing the most used words for each language.

<img src="/readme_src/word_counting_multi.png" height="400px" ></img>
<br>
<br>

### How to run the code on your PC (Unix)

1. install MongoDB (https://docs.mongodb.com/manual/administration/install-on-linux/)

2. unzip the database *./db/mongoDB_backup/db_compressed.rar* file

3. import the database on MongoDB using the command from the main directory `mongorestore -d brexit ./db/mongoDB_backup/brexit/ -u Admin -p Password --authenticationDatabase admin`

4. add some twitter api credentials in the file *./twitter-analyzers/credentials.csv* (
the more accounts you use, the higher is the interaction throghput)

5. run *./twitter-analyzers/multithread-tweets-analyzer.py* for getting new tweets data

6. run some python scripts inside the *./analysis_scripts/* folder to get some plot 
