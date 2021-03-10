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
               ./structure-spark-kafka-batch.py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, json_tuple, get_json_object, to_json, concat_ws
from pyspark.sql.functions import struct
from pyspark.sql.functions import window
from pyspark.sql.types import *


def main():
    print_data_frame = True
    output_format_is_json = True

    spark = SparkSession.builder \
        .appName("MyTest") \
        .getOrCreate()

    raw_data = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "spark-test") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    if print_data_frame:
        '''
        +----+------------------+----------+---------+------+-----------------------+-------------+
        |key |value             |topic     |partition|offset|timestamp              |timestampType|
        +----+------------------+----------+---------+------+-----------------------+-------------+
        |null|[7B 22 69 ... ...]|spark-test|0        |0     |2021-03-04 01:26:23.068|0            |
        |null|[7B 22 69 ... ...]|spark-test|0        |1     |2021-03-04 01:26:23.078|0            |
        '''
        raw_data.show(100, False)

    # selectExpr projects a set of SQL expressions and returns a new DataFrame, e.g.
    #     data_frame.selectExpr("column_1/2 as new_column_1", "round(column_2/column_1) as new_column_2")
    raw_data = raw_data.selectExpr("offset", "timestamp", "CAST(value AS STRING) as json_str").cache()

    # explode can convert a map/array column to multi rows
    """
    from pyspark.sql.functions import explode
    from pyspark.sql.functions import split
    data = [("ID_1", "A,B,C"), ("ID_2", "D,E,F")]
    df = spark.createDataFrame(data, ("key", "value"))
    df.select(df.key, explode(split(df.value, ",")).alias("value")).show()
    
    +----+-----+
    | key|value|
    +----+-----+
    |ID_1|    A|
    |ID_1|    B|
    |ID_1|    C|
    |ID_2|    D|
    |ID_2|    E|
    |ID_2|    F|
    +----+-----+
    """

    json_schema = StructType(
        [
            StructField("id", StringType()),
            StructField("time", TimestampType(), True),
            StructField("value", IntegerType(), True)
        ]
    )
    # from_json parse a string column to a json (map) column
    json_column_data = raw_data.select(raw_data.offset,
                                       from_json(raw_data.json_str, schema=json_schema).alias("data"))
    # change the map column to multi-column
    # without alias the column name is data.id
    json_data = json_column_data.select(json_column_data.offset,
                                        json_column_data.data.id.alias("id"),
                                        json_column_data.data.time.alias("time"),
                                        json_column_data.data.value.alias("value"))
    if print_data_frame:
        '''
        +------+---------------------------------+
        |offset|data                             |
        +------+---------------------------------+
        |0     |[id-001, 2020-01-01 08:00:01, 11]|
        |1     |[id-002, 2020-01-01 08:00:02, 12]|
        |2     |[id-003, 2020-01-01 08:00:03, 13]|
        '''
        json_column_data.show(100, False)
        '''
        +------+------+-------------------+-----+
        |offset|id    |time               |value|
        +------+------+-------------------+-----+
        |0     |id-001|2020-01-01 08:00:01|11   |
        |1     |id-002|2020-01-01 08:00:02|12   |
        |2     |id-003|2020-01-01 08:00:03|13   |
        '''
        json_data.show(100, False)

    # from_json also can parse the nested json data
    """
    data = [("1", '''{"f1": "value1", "f2": {"f3": "value2"}}'''), ("2", '''{"f1": "value12"}''')]
    df = spark.createDataFrame(data, ("key", "value"))
    json_schema = StructType(
        [
            StructField("f1", StringType()),
            StructField("f2", StructType([StructField("f3", StringType())]), True)
        ]
    )
    json_column_data = df.select(df.key, 
                                 from_json(df.value, schema=json_schema).alias("data"))
    json_data = json_column_data.select(json_column_data.data.f1.alias("f1"),
                                        json_column_data.data.f2.f3.alias("f3"))
    json_data.show(100, False)
    
    +-------+------+
    |f1     |f3    |
    +-------+------+
    |value1 |value2|
    |value12|null  |
    +-------+------+
    """

    # json_tuple parse a json string column to multi-column
    # (without alias, the column will be c0, c1, c2, ...)
    json_data = raw_data.select(raw_data.offset,
                                json_tuple(raw_data.json_str, "id", "time", "value").alias("id", "time", "value"))
    if print_data_frame:
        '''
        +------+------+-------------------+-----+
        |offset|id    |time               |value|
        +------+------+-------------------+-----+
        |0     |id-001|2020-01-01 08:00:01|11   |
        |1     |id-002|2020-01-01 08:00:02|12   |
        |2     |id-003|2020-01-01 08:00:03|13   |
        '''
        json_data.show(100, False)

    # get_json_object create a column base on the json column and json path
    json_data = raw_data.select(
        raw_data.offset,
        get_json_object(raw_data.json_str, "$.id").alias("id"),
        get_json_object(raw_data.json_str, "$.time").alias("time"),
        get_json_object(raw_data.json_str, "$.value").alias("value").cast(IntegerType())
    ).cache()
    if print_data_frame:
        '''
        +------+------+-------------------+-----+
        |offset|id    |time               |value|
        +------+------+-------------------+-----+
        |0     |id-001|2020-01-01 08:00:01|11   |
        |1     |id-002|2020-01-01 08:00:02|12   |
        |2     |id-003|2020-01-01 08:00:03|13   |
        '''
        json_data.show(100, False)

    # get_json_object also can parse the nested json data
    """
    data = [("1", '''{"f1": "value1", "f2": {"f3": "value2"}}'''), ("2", '''{"f1": "value12"}''')]
    df = spark.createDataFrame(data, ("key", "value"))
    df.select(df.key,
              get_json_object(df.value, '$.f1').alias("c1"),
              get_json_object(df.value, '$.f2.f3').alias("c2")
    ).show()
    
    +---+-------+------+
    |key|     c1|    c2|
    +---+-------+------+
    |  1| value1|value2|
    |  2|value12|  null|
    +---+-------+------+
    """

    if print_data_frame:
        '''
        +------+------+-------------------+-----+
        |offset|id    |time               |value|
        +------+------+-------------------+-----+
        |3     |id-001|2020-01-01 08:25:01|21   |
        |4     |id-002|2020-01-01 08:25:02|22   |
        '''
        json_data.select("*").where("value > 20").show(100, False)

        '''
        +------+------+-----+
        |offset|id    |value|
        +------+------+-----+
        |3     |id-001|21   |
        |4     |id-002|22   |
        '''
        json_data.select("offset", "id", "value").where("value > 20").show(100, False)

        '''
        +------+-----+
        |id    |count|
        +------+-----+
        |id-001|3    |
        |id-003|3    |
        |id-002|3    |
        +------+-----+
        '''
        json_data.where("value > 20").groupBy("id").count().show(100, False)

        '''
        +------+----------+
        |id    |avg(value)|
        +------+----------+
        |id-001|31.0      |
        |id-003|33.0      |
        |id-002|32.0      |
        +------+----------+
        '''
        json_data.where("value > 20").groupBy("id").avg("value").show(100, False)

        '''
        +------------------------------------------+------+---------+
        |window                                    |id    |avg_value|
        +------------------------------------------+------+---------+
        |[2020-01-01 08:00:00, 2020-01-01 08:15:00]|id-001|11.0     |
        |[2020-01-01 08:00:00, 2020-01-01 08:15:00]|id-002|12.0     |
        |[2020-01-01 08:00:00, 2020-01-01 08:15:00]|id-003|13.0     |
        |[2020-01-01 08:15:00, 2020-01-01 08:30:00]|id-001|21.0     |
        |[2020-01-01 08:15:00, 2020-01-01 08:30:00]|id-002|22.0     |
        |[2020-01-01 08:15:00, 2020-01-01 08:30:00]|id-003|23.0     |
        |[2020-01-01 08:45:00, 2020-01-01 09:00:00]|id-001|31.0     |
        |[2020-01-01 08:45:00, 2020-01-01 09:00:00]|id-002|32.0     |
        |[2020-01-01 08:45:00, 2020-01-01 09:00:00]|id-003|33.0     |
        |[2020-01-01 09:00:00, 2020-01-01 09:15:00]|id-001|41.0     |
        |[2020-01-01 09:00:00, 2020-01-01 09:15:00]|id-002|42.0     |
        |[2020-01-01 09:00:00, 2020-01-01 09:15:00]|id-003|43.0     |
        +------------------------------------------+------+---------+
        '''
        result_data = json_data.groupBy(
            window(json_data.time, '15 minutes', '15 minutes'),
            json_data.id
        ).avg("value").orderBy('window', 'id').withColumnRenamed("avg(value)", "avg_value").cache()

        result_data.show(100, False)

        '''
        +--------------+------------------------------------------+
        |max(avg_value)|min(window)                               |
        +--------------+------------------------------------------+
        |43.0          |[2020-01-01 08:00:00, 2020-01-01 08:15:00]|
        +--------------+------------------------------------------+
        '''
        result_data.agg({"avg_value": "max", "window": "min"}).show(100, False)

    result_data_frame = json_data.groupBy(
        window(json_data.time, '15 minutes', '15 minutes'),
        json_data.id
    ).avg("value").orderBy('window', 'id').withColumnRenamed("avg(value)", "avg_value")

    if output_format_is_json is True:
        result_data_frame = result_data_frame.select(
            to_json(
                struct(
                    result_data_frame.window.cast(StringType()).alias("window"),
                    result_data_frame.id,
                    result_data_frame.avg_value
                )
            ).alias("value")
        )
        '''
        +--------------------------------------------------------------------------------------+
        |value                                                                                 |
        +--------------------------------------------------------------------------------------+
        |{"window":"[2020-01-01 08:00:00, 2020-01-01 08:15:00]","id":"id-001","avg_value":11.0}|
        |{"window":"[2020-01-01 08:00:00, 2020-01-01 08:15:00]","id":"id-002","avg_value":12.0}|
        |{"window":"[2020-01-01 08:00:00, 2020-01-01 08:15:00]","id":"id-003","avg_value":13.0}|
        |{"window":"[2020-01-01 08:15:00, 2020-01-01 08:30:00]","id":"id-001","avg_value":21.0}|
        |{"window":"[2020-01-01 08:15:00, 2020-01-01 08:30:00]","id":"id-002","avg_value":22.0}|
        |{"window":"[2020-01-01 08:15:00, 2020-01-01 08:30:00]","id":"id-003","avg_value":23.0}|
        |{"window":"[2020-01-01 08:45:00, 2020-01-01 09:00:00]","id":"id-001","avg_value":31.0}|
        |{"window":"[2020-01-01 08:45:00, 2020-01-01 09:00:00]","id":"id-002","avg_value":32.0}|
        |{"window":"[2020-01-01 08:45:00, 2020-01-01 09:00:00]","id":"id-003","avg_value":33.0}|
        |{"window":"[2020-01-01 09:00:00, 2020-01-01 09:15:00]","id":"id-001","avg_value":41.0}|
        |{"window":"[2020-01-01 09:00:00, 2020-01-01 09:15:00]","id":"id-002","avg_value":42.0}|
        |{"window":"[2020-01-01 09:00:00, 2020-01-01 09:15:00]","id":"id-003","avg_value":43.0}|
        +--------------------------------------------------------------------------------------+
        '''
        result_data_frame.show(100, False)
    else:
        # concat_ws concatenates multiple input string columns together into a single string column,
        # using the given separator.
        result_data_frame = result_data_frame.select(
            concat_ws(
                "##",
                result_data_frame.window.cast(StringType()),
                result_data_frame.id,
                result_data_frame.avg_value
            )
            .cast(StringType())
            .alias("value")
        )
        '''
        +--------------------------------------------------------+
        |value                                                   |
        +--------------------------------------------------------+
        |[2020-01-01 08:00:00, 2020-01-01 08:15:00]##id-001##11.0|
        |[2020-01-01 08:00:00, 2020-01-01 08:15:00]##id-002##12.0|
        |[2020-01-01 08:00:00, 2020-01-01 08:15:00]##id-003##13.0|
        |[2020-01-01 08:15:00, 2020-01-01 08:30:00]##id-001##21.0|
        |[2020-01-01 08:15:00, 2020-01-01 08:30:00]##id-002##22.0|
        |[2020-01-01 08:15:00, 2020-01-01 08:30:00]##id-003##23.0|
        |[2020-01-01 08:45:00, 2020-01-01 09:00:00]##id-001##31.0|
        |[2020-01-01 08:45:00, 2020-01-01 09:00:00]##id-002##32.0|
        |[2020-01-01 08:45:00, 2020-01-01 09:00:00]##id-003##33.0|
        |[2020-01-01 09:00:00, 2020-01-01 09:15:00]##id-001##41.0|
        |[2020-01-01 09:00:00, 2020-01-01 09:15:00]##id-002##42.0|
        |[2020-01-01 09:00:00, 2020-01-01 09:15:00]##id-003##43.0|
        +--------------------------------------------------------+
        '''
        result_data_frame.show(100, False)

    # result_data_frame must contains columns key(optional), value(required), topic(optional)
    # otherwise it can't be sent to Kafka
    result_data_frame \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "spark-test-output") \
        .save()


if __name__ == "__main__":
    main()
