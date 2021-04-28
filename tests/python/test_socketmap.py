import unittest
from pyspark.sql import SparkSession
from style.socketmap import socketmap
from pycorenlp import StanfordCoreNLP


def parse_sentence(sentence):
    nlp = StanfordCoreNLP('http://localhost:9000')
    response = nlp.annotate(
        sentence,
        properties={'annotators': 'parse', 'outputFormat': 'json'},
    )
    return response['sentences'][0]['parse']


def compare_unordered_dataframes(left, right):
    left, right = (df.sort(*sorted(df.columns)) for df in (left, right))
    return left.toPandas().equals(right.toPandas())


class TestSocketmap(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.getOrCreate()

    def test_concat(self):
        df = self.spark.createDataFrame([['a'], ['b']], ['c'])
        tru = self.spark.createDataFrame([['ad'], ['bd']], ['c'])
        est = socketmap(self.spark, df, lambda x: {'c': x['c'] + 'd'})
        self.assertEqual(compare_unordered_dataframes(tru, est), True)

    def test_corenlp(self):
        sentences = [
            ['The ball is red.'],
            ['I went to the store.'],
            ['There is a wisdom that is a woe.'],
        ]
        df = self.spark.createDataFrame(sentences, ['sentence'])
        tru = self.spark.createDataFrame(
                map(lambda a: [parse_sentence(a[0])], sentences),
            ['tree'],
        )
        est = socketmap(
            self.spark,
            df,
            lambda row: {'tree': parse_sentence(row['sentence'])},
        )
        print('\n\n\nINSPECT EST')
        est.show()
        print('\n\n\nINSPECT TRU')
        tru.show()
        self.assertEqual(compare_unordered_dataframes(tru, est), True)


if __name__ == '__main__':
    unittest.main()
