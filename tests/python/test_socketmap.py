import unittest
from pyspark.sql import SparkSession
from socketmap import socketmap
from pycorenlp import StanfordCoreNLP


def concat(input_rows_iterator):
    outputs = []
    for row in input_rows_iterator:
        output = {'c': row['c'] + 'd'}
        outputs.append(output)
    return outputs


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


def compare_unordered_dataframes(left, right):
    left, right = (df.sort(*sorted(df.columns)) for df in (left, right))
    return left.toPandas().equals(right.toPandas())


class TestSocketmap(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.getOrCreate()

    def test_concat(self):
        df = self.spark.createDataFrame([['a'], ['b']], ['c'])
        tru = self.spark.createDataFrame([['ad'], ['bd']], ['c'])
        est = socketmap(self.spark, df, concat)
        self.assertEqual(compare_unordered_dataframes(tru, est), True)

    def test_corenlp(self):
        sentences = [
            ['The ball is red.'],
            ['I went to the store.'],
            ['There is a wisdom that is a woe.'],
        ]
        df = self.spark.createDataFrame(sentences, ['sentence'])
        dicts = [{'sentence': sentence[0]} for sentence in sentences]
        tru = self.spark.createDataFrame(
            [[output['tree']] for output in parse_sentences(dicts)],
            ['tree'],
        )
        est = socketmap(self.spark, df, parse_sentences)
        self.assertEqual(compare_unordered_dataframes(tru, est), True)

    def test_empty(self):
        column = 'test'
        schema = self.spark.createDataFrame([[column]], [column]).schema
        df = self.spark.createDataFrame([], schema)
        identity = lambda row: {column: row[column]}
        tru = None
        est = socketmap(self.spark, df, identity)
        self.assertEqual(tru, est)


if __name__ == '__main__':
    unittest.main()
