# socketmap
High-level PySpark tool for applying server-dependent functions

## Source Dependencies (Tested on Ubuntu 20.04)
#### PostgreSQL
```
sudo apt install postegresql
```
#### PySpark
1. Go to https://spark.apache.org/downloads.html
2. Select package type "Pre-built for Apache Hadoop 3.2 or later"
3. Download and extract the tarball
4. Run the following
```
cd spark-3.1.1-bin-hadoop3.2/python
python3 setup.py sdist
sudo python3 -m pip install sdist/*.tar.gz
```
## Test Dependencies
#### Stanford Core NLP
```
wget http://nlp.stanford.edu/software/stanford-corenlp-latest.zip
unzip stanford-corenlp-latest.zip
export STANFORD_NLP_PATH=$PWD/stanford-corenlp-4.2.0
sudo python3 -m pip install pycorenlp
```
## Installation
```
sudo python3 -m pip install socketmap
```
## Tests
```
bash tests/shell/test_socketmap.sh
```
## Example
```
from pyspark.sql import SparkSession
from pycorenlp import StanfordCoreNLP
from socketmap import socketmap


def parse_sentence(sentence):
    nlp = StanfordCoreNLP('http://localhost:9000')
    response = nlp.annotate(
        sentence,
        properties={'annotators': 'parse', 'outputFormat': 'json'},
    )
    return response['sentences'][0]['parse']


spark = SparkSession.builder.getOrCreate()
sentences = [
    ['The ball is red.'],
    ['I went to the store.'],
    ['There is a wisdom that is a woe.'],
]
input_dataframe = spark.createDataFrame(sentences, ['sentence'])
wrapper = lambda row: {'tree': parse_sentence(row['sentence'])}
output_dataframe = socketmap(spark, input_dataframe, wrapper)
```
