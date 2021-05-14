# socketmap
High-level PySpark tool for applying server-dependent functions

## Source Dependencies (Tested on Ubuntu 20.04)

#### PostgreSQL
```bash
sudo apt install postegresql
```

#### PySpark
1. Go to https://spark.apache.org/downloads.html
2. Select package type "Pre-built for Apache Hadoop 3.2 or later"
3. Download and extract the tarball
4. Run the following
```bash
cd spark-3.1.1-bin-hadoop3.2/python
python3 setup.py sdist
sudo python3 -m pip install sdist/*.tar.gz
```

## Test Dependencies

#### Stanford Core NLP
```bash
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
```bash
bash tests/shell/test_socketmap.sh
```

## Example

#### Python source script
```python
from pyspark.sql import SparkSession
from pycorenlp import StanfordCoreNLP
from socketmap import socketmap


def parse_sentences(input_rows_iterator):
    nlp = StanfordCoreNLP('http://localhost:9000')
    outputs = []
    for row in input_rows_iterator:
        sentence = row['sentence']
        response = nlp.annotate(
            sentence,
            properties={'annotators': 'parse', 'outputFormat': 'json'},
        )
        output = {'tree': response['sentences'][0]['parse']}
        outputs.append(output)
    return outputs


spark = SparkSession.builder.getOrCreate()
sentences = [
    ['The ball is red.'],
    ['I went to the store.'],
    ['There is a wisdom that is a woe.'],
]
input_dataframe = spark.createDataFrame(sentences, ['sentence'])
output_dataframe = socketmap(spark, input_dataframe, parse_sentences)
```

#### Spark driver
```bash
DRIVER_CORES=32
APP_NAME=example
DRIVER_MEMORY=160g
EXECUTOR_MEMORY=3g

# run corenlp server
CURDIR=$PWD
cd $STANFORD_NLP_PATH
java -mx4g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -port 9000 -timeout 15000 &
cd $CURDIR

sudo runuser -l postgres -c "source $HOME/paths && $SPARK_HOME/bin/spark-submit \
    --name $APP_NAME \
    --driver-cores $DRIVER_CORES \
    --driver-memory $DRIVER_MEMORY \
    --executor-memory $EXECUTOR_MEMORY \
    ${HOME}/socketmap/scripts/python/parse_sentences.py"
```
