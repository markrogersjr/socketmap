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
print('INPUT DATAFRAME')
input_dataframe = spark.createDataFrame(sentences, ['sentence'])
input_dataframe.show()
wrapper = lambda row: {'tree': parse_sentence(row['sentence'])}
output_dataframe = socketmap(spark, input_dataframe, wrapper)
print('OUTPUT DATAFRAME')
output_dataframe.show()
