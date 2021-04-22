# Big Data - Final Project

### This purpose of this repo is to process text using Databricks and PySpark.

### Author
[Shiva Rama Krishna Vodnala](https://github.com/srkvodnala)

### Source for text data

[Hindu Magic, by Hereward Carrington](https://www.gutenberg.org/files/65121/65121-0.txt)

### Tools, Languages
* Databricks Community.
* Spark Processing Engine.
* PySpark.
* Python.


### Published Notebook

[Databricks published notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/290691640627521/86055512989159/4160698203358490/latest.html)


### Commands Used 

### Data Injection

* We need to import urllib.request to read data from url.

* With this we can store data temporarily.

``` 
import urllib.request
stringInURL = "https://www.gutenberg.org/files/65121/65121-0.txt"
urllib.request.urlretrieve(stringInURL, "/tmp/shiva.txt") 
```

* With this we can move the temporary file in the databricks storage folder by using dbutils.fs.mv

```
dbutils.fs.mv("file:/tmp/shiva.txt", "dbfs:/data/HinduMagic.txt")
```

* Later, we need to move file to spark using sparkContext.

```
HinduMagicRDD = sc.textFile("dbfs:/data/HinduMagic.txt")
```

### Cleaning Data

* Using flatMap break the data ans convert the given text to lowercase and remove the empty spaves and then divide the text into terms.
```
rawRDD = HinduMagicRDD.flatMap(lambda line : line.lower().strip().split(" "))
```

* Import re(Regular Expression) library to remove the punctutation
```
import re
cleanTokensRDD = rawRDD.map(lambda w1: re.sub(r'[^A-Za-z]', '', w1))
```
* To remove the stopwords we need to use pyspark.ml.feature by importing stopwordsRemover.
```
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover()
stopwords = remover.getStopWords()
cleanWordsRDD = cleanTokensRDD.filter(lambda word: word not in stopwords)
```
* Below command removes the empty spaces.
```
removespacesRDD = rawRDD.filter(lambda x: x != "")
```

### Data Processing

* During this stage to process data we are going to map the words to key value pairs.
```
KeyValuePairsRDD= removespacesRDD.map(lambda word: (word,1))
```

* To get the word count by(word,count) use reduceByKey() 
```
wordsCountRDD = KeyValuePairsRDD.reduceByKey(lambda acc, value: acc+value)
```

* Prints the first 10 words in descending order by using sortbykey()
```
HinduMagic = wordsCountRDD.map(lambda x: (x[1], x[0])).sortByKey(False).take(10)
print(HinduMagic)
```
### Charting
* For visualization now we are going to use Pandas, MatPlotLib, and Seaborn.
```
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

source = 'Hindu Magic, by Hereward Carrington'
title = 'Top Words Ten in ' + source
xlabel = 'Count'
ylabel = 'Words'

df = pd.DataFrame.from_records(HinduMagic, columns =[xlabel, ylabel]) 
plt.figure(figsize=(10,5))
sns.barplot(xlabel, ylabel, data=df, palette="magma").set_title(title)
```

### Results
![img1](https://raw.githubusercontent.com/srkvodnala/bigdata-finalproject/main/BDimg1.PNG)
![img2](https://raw.githubusercontent.com/srkvodnala/bigdata-finalproject/main/BDimg2.PNG)

### WordCloud
1. Download nltk, wordcloud libraries
```
pip install nltk
```

```
pip install wordcloud
```

```
nltk.download('popular')
```
2. Create a wordcloud using above downloaded libraries

```
import wordcloud
import nltk
import matplotlib.pyplot as plt

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from wordcloud import WordCloud

class WordCloudGeneration:
    def preprocessing(self, data):
        data = [item.lower() for item in data]
        stop_words = set(stopwords.words('english'))
        paragraph = ' '.join(data)
        word_tokens = word_tokenize(paragraph)  
        preprocessed_data = ' '.join([word for word in word_tokens if not word in stop_words])
        print("\n Preprocessed Data: " ,preprocessed_data)
        return preprocessed_data

    def create_word_cloud(self, final_data):
        wordcloud = WordCloud(width=1600, height=800, max_words=10, max_font_size=200, background_color="white").generate(final_data)
        plt.figure(figsize=(12,10))
        plt.imshow(wordcloud)
        plt.axis("off")
        plt.show()

wordcloud_generator = WordCloudGeneration()
import urllib.request
url = "https://www.gutenberg.org/files/65121/65121-0.txt"
request = urllib.request.Request(url)
response = urllib.request.urlopen(request)
input_text = response.read().decode('utf-8')

input_text = input_text.split('.')
clean_data = wordcloud_generator.preprocessing(input_text)
wordcloud_generator.create_word_cloud(clean_data)
```

![img3](https://raw.githubusercontent.com/srkvodnala/bigdata-finalproject/main/BDimg3.PNG)


### References
1. https://seaborn.pydata.org/tutorial/color_palettes.html
2. https://www.w3schools.com/python/python_lambda.asp
3. https://datascience-enthusiast.com/Python/cs110_lab3a_word_count_rdd.html

