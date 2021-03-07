# coding=utf-8
"""
参考资料
  http://spark.apache.org/docs/2.4.0/structured-streaming-kafka-integration.html

启动 ZooKeeper 和 Kafka
  cd ~/software/zookeeper-3.4.13
  ./bin/zkServer.sh start

  cd ~/software/kafka_2.12-2.2.0
  ./bin/kafka-server-start.sh config/server.properties

创建 topic
  bin/kafka-topics.sh --create \
                      --topic spark-test \
                      --replication-factor 1 \
                      --partitions 1 \
                      --bootstrap-server localhost:9092

  bin/kafka-topics.sh --list --bootstrap-server localhost:9092

发送消息
  ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic spark-test
    {"id":"id-001", "time":"2020-01-01 08:00:01", "value":11}
    {"id":"id-002", "time":"2020-01-01 08:00:02", "value":12}
    {"id":"id-003", "time":"2020-01-01 08:00:03", "value":13}
    {"id":"id-001", "time":"2020-01-01 08:25:01", "value":21}
    {"id":"id-002", "time":"2020-01-01 08:25:02", "value":22}
    {"id":"id-003", "time":"2020-01-01 08:25:03", "value":23}
    {"id":"id-001", "time":"2020-01-01 08:50:01", "value":31}
    {"id":"id-002", "time":"2020-01-01 08:50:02", "value":32}
    {"id":"id-003", "time":"2020-01-01 08:50:03", "value":33}
    {"id":"id-001", "time":"2020-01-01 09:00:01", "value":41}
    {"id":"id-002", "time":"2020-01-01 09:00:02", "value":42}
    {"id":"id-003", "time":"2020-01-01 09:00:03", "value":43}
    ... ...

查看消息
  ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic spark-test --from-beginning

Kafka 的 topic 的指定方式
  assign: 以 JSON 格式指定要读取的 topic 和 partition，比如 {"topicA":[0,1],"topicB":[2,4]}
  subscribe: 以 String 指定要读取的 topic，用逗号隔开，比如 "topicA,topicB"
  subscribePattern: 使用正则表达式匹配 topic，比如 "topic*"

Kafka 的 offset 的指定方式
  .option("startingOffsets", "earliest")
  .option("endingOffsets", "latest")
  .option("startingOffsets", '''{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}''')
  .option("endingOffsets", '''{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}''')

  Json 格式下，-2 代表 earliest，-1 代表 latest

  batch 的 startingOffsets 不能是 latest
  batch 的 endingOffsets 不能是 earliest
  streaming 没有 endingOffsets

读到 DataFrame 的每一列数据格式如下
  key            binary
  value          binary
  topic          string
  partition      int
  offset         long
  timestamp      long
  timestampType  int

启动 pyspark 做测试
  注意 scala 版本，可以通过 spark-shell 查看
  ./pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6

submit local
  spark-submit --master local \
               --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 \
               ./structure-spark-kafka-streaming.py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import get_json_object, concat_ws
from pyspark.sql.functions import window
from pyspark.sql.types import *


def main():
    sink = "Kafka"

    spark = SparkSession.builder \
        .appName("MyTest") \
        .getOrCreate()

    raw_data = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "spark-test") \
        .option("startingOffsets", "earliest") \
        .load()

    # Note:
    #   Queries with streaming sources must be executed with writeStream.start()
    #   so raw_data.show(100, False) is not allowed without writeStream.start()

    raw_data = raw_data.selectExpr("offset", "timestamp", "CAST(value AS STRING) as json_str")

    # get_json_object create a column base on the json column and json path
    # the json can be complicated with nested fields
    # e.g.
    #    data = [("1", '''{"f1": "value1", "f2": {"f3": "value2"}}'''), ("2", '''{"f1": "value12"}''')]
    #    df = spark.createDataFrame(data, ("key", "value"))
    #    df.select(df.key,
    #              get_json_object(df.value, '$.f1').alias("c1"),
    #              get_json_object(df.value, '$.f2.f3').alias("c2")
    #    ).show()
    json_data = raw_data.select(
        raw_data.offset,
        get_json_object(raw_data.json_str, "$.id").alias("id"),
        get_json_object(raw_data.json_str, "$.time").alias("time").cast(TimestampType()),
        get_json_object(raw_data.json_str, "$.value").alias("value").cast(IntegerType())
    )

    # Sorting is not supported on streaming DataFrames/Datasets,
    # unless it is on aggregated DataFrame/DataSet in Complete output mode
    aggregate_data = json_data \
        .withWatermark("time", "20 minutes") \
        .groupBy(
            window(json_data.time, '15 minutes', '10 minutes'),
            json_data.id
        ) \
        .avg("value") \
        .withColumnRenamed("avg(value)", "avg_value")

    if sink == "Kafka":
        # concat_ws concatenates multiple input string columns together into a single string column,
        # using the given separator.
        result_data_frame = aggregate_data.select(
            concat_ws(
                "##",
                aggregate_data.window.cast(StringType()),
                aggregate_data.id,
                aggregate_data.avg_value
            )
            .cast(StringType())
            .alias("value")
        )

        # Checkpoint location:
        #   For some output sinks where the end-to-end fault-tolerance can be guaranteed,
        #   specify the location where the system will write all the checkpoint information.
        #   This should be a directory in an HDFS-compatible fault-tolerant file system.

        # result_data_frame must contains columns key(optional), value(required), topic(optional)
        # otherwise it can't be sent to Kafka
        query = result_data_frame \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "spark-test-output") \
            .option("checkpointLocation", "checkpoint") \
            .outputMode("update") \
            .trigger(processingTime='2 seconds') \
            .start()

        '''
        Kafka topic spark-test-output receive messages
        ==============================================
        [2020-01-01 08:50:00, 2020-01-01 09:05:00]##id-002##37.0
        [2020-01-01 08:50:00, 2020-01-01 09:05:00]##id-003##38.0
        [2020-01-01 08:40:00, 2020-01-01 08:55:00]##id-003##33.0
        [2020-01-01 08:40:00, 2020-01-01 08:55:00]##id-001##31.0
        [2020-01-01 08:40:00, 2020-01-01 08:55:00]##id-002##32.0
        [2020-01-01 08:20:00, 2020-01-01 08:35:00]##id-003##23.0
        [2020-01-01 08:00:00, 2020-01-01 08:15:00]##id-001##11.0
        [2020-01-01 08:00:00, 2020-01-01 08:15:00]##id-002##12.0
        [2020-01-01 09:00:00, 2020-01-01 09:15:00]##id-001##41.0
        [2020-01-01 08:20:00, 2020-01-01 08:35:00]##id-002##22.0
        [2020-01-01 08:00:00, 2020-01-01 08:15:00]##id-003##13.0
        [2020-01-01 07:50:00, 2020-01-01 08:05:00]##id-001##11.0
        [2020-01-01 09:00:00, 2020-01-01 09:15:00]##id-002##42.0
        [2020-01-01 08:20:00, 2020-01-01 08:35:00]##id-001##21.0
        [2020-01-01 07:50:00, 2020-01-01 08:05:00]##id-003##13.0
        [2020-01-01 08:50:00, 2020-01-01 09:05:00]##id-001##36.0
        [2020-01-01 07:50:00, 2020-01-01 08:05:00]##id-002##12.0
        [2020-01-01 09:00:00, 2020-01-01 09:15:00]##id-003##43.0
        '''
    else:
        query = aggregate_data \
            .writeStream \
            .foreachBatch(foreach_batch_function) \
            .outputMode("complete") \
            .trigger(processingTime='2 seconds') \
            .start()

    query.awaitTermination()


def foreach_batch_function(batch_df, epoch_id):
    print("\n\n\n\n\n")
    print("==================")
    print(epoch_id)
    print("==================")
    print("\n\n\n\n\n")
    batch_df.orderBy('window', 'id').show(100, False)
    '''
    +------------------------------------------+------+---------+
    |window                                    |id    |avg_value|
    +------------------------------------------+------+---------+
    |[2020-01-01 07:50:00, 2020-01-01 08:05:00]|id-001|11.0     |
    |[2020-01-01 07:50:00, 2020-01-01 08:05:00]|id-002|12.0     |
    |[2020-01-01 07:50:00, 2020-01-01 08:05:00]|id-003|13.0     |
    |[2020-01-01 08:00:00, 2020-01-01 08:15:00]|id-001|11.0     |
    |[2020-01-01 08:00:00, 2020-01-01 08:15:00]|id-002|12.0     |
    |[2020-01-01 08:00:00, 2020-01-01 08:15:00]|id-003|13.0     |
    |[2020-01-01 08:20:00, 2020-01-01 08:35:00]|id-001|21.0     |
    |[2020-01-01 08:20:00, 2020-01-01 08:35:00]|id-002|22.0     |
    |[2020-01-01 08:20:00, 2020-01-01 08:35:00]|id-003|23.0     |
    |[2020-01-01 08:40:00, 2020-01-01 08:55:00]|id-001|31.0     |
    |[2020-01-01 08:40:00, 2020-01-01 08:55:00]|id-002|32.0     |
    |[2020-01-01 08:40:00, 2020-01-01 08:55:00]|id-003|33.0     |
    |[2020-01-01 08:50:00, 2020-01-01 09:05:00]|id-001|36.0     |
    |[2020-01-01 08:50:00, 2020-01-01 09:05:00]|id-002|37.0     |
    |[2020-01-01 08:50:00, 2020-01-01 09:05:00]|id-003|38.0     |
    |[2020-01-01 09:00:00, 2020-01-01 09:15:00]|id-001|41.0     |
    |[2020-01-01 09:00:00, 2020-01-01 09:15:00]|id-002|42.0     |
    |[2020-01-01 09:00:00, 2020-01-01 09:15:00]|id-003|43.0     |
    +------------------------------------------+------+---------+
    '''


if __name__ == "__main__":
    main()
