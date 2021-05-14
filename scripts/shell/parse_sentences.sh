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
