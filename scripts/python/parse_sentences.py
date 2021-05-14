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
print('INPUT DATAFRAME')
input_dataframe = spark.createDataFrame(sentences, ['sentence'])
input_dataframe.show()
output_dataframe = socketmap(spark, input_dataframe, parse_sentences)
print('OUTPUT DATAFRAME')
output_dataframe.show()
