/**
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

读到 Dataset 的每一列数据格式如下
  key            binary
  value          binary
  topic          string
  partition      int
  offset         long
  timestamp      long
  timestampType  int

submit local
  ./spark-submit --master local \
                 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 \
                 --class com.example.demo.StructuredSparkKafkaStreaming \
                 structured-spark-kafka-streaming_2.11-0.1.jar

Window 环境下，如果写文件到 local 可能会出错 (不仅是写文件的 action，有的操作会自己写临时文件)
  Exception in thread "main" java.io.IOException: (null) entry in command string: null chmod 0644 C:\Users\XXX

  解决方法
      1. 上网下载 winutils.exe
      2. 创建目录比如 C:\hadoop\bin，然后将 winutils.exe 复制到 C:\hadoop\bin
      3. 创建系统环境变量 HADOOP_HOME，变量的值是 C:\hadoop
      4. 重启 IDEA
*/
package com.example.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.{concat_ws, from_json, struct, to_json, window}
import org.apache.spark.sql.types._


object StructuredSparkKafkaStreaming {

  // ==================================
  // case class 要定义在引用 case class 的函数 (这个例子里是 main 函数) 的外面
  // ==================================
  case class Data_1(id: String, name: String, value: Int)
  case class Data_2(key: String, value: String)

  def main(args: Array[String]): Unit = {
  
    // INFO 日志的量非常大，改成 WARN level 方便查看运行结果
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val outputFormatIsJson = true
    val sink = "Kafka"
    val defineForeachFunction = false

    val spark = SparkSession.builder
                            .appName("StructuredSparkKafkaStreaming")
                            .master("local[2]")
                            .getOrCreate()

    // ====================================
    // Seq 不是 spark 的类，而是 Scala 的类
    // 必须导入 spark 的 implicits (隐式) 才能使 Seq 有 toDS 函数可以转成 DataSet
    // 同时才可以在 select 函数中用 $"name" 表达 Column 类
    // ====================================
    import spark.implicits._

    val ds_1 = Seq(Data_1("id_1", "name_a", 100),
                   Data_1("id_2", "name_b", 200),
                   Data_1("id_3", "name_c", 300)).toDS().cache()

    ds_1.show(100, truncate = false)
    ds_1.select("id", "name", "value").show()
    ds_1.select("name", "value").show()
    ds_1.select($"name", ($"value" + 1).alias("new_value")).show()
    ds_1.filter($"value" > 150).show()

    val kafkaData = spark.readStream
                         .format("kafka")
                         .option("kafka.bootstrap.servers", "localhost:9092")
                         .option("subscribe", "spark-test")        // or .option("assign", """{"spark-test":[0]}""")
                         .option("startingOffsets", "earliest")
                         .load()

    // ===============================
    // Note:
    //   Queries with streaming sources must be executed with writeStream.start()
    //   so rawData.show(100, false) is not allowed without writeStream.start()
    // ===============================

    // selectExpr projects a set of SQL expressions and returns a new DataFrame, e.g.
    //     data_set.selectExpr("column_1/2 as new_column_1", "round(column_2/column_1) as new_column_2")
    val rawData = kafkaData.selectExpr("offset", "timestamp", "CAST(value AS STRING) as json_str")

    val jsonSchema = new StructType(
      Array(
        StructField("id", StringType),
        StructField("time", TimestampType, nullable = true),
        StructField("value", IntegerType, nullable = true)
      )
    )

    // from_json parse a string column to a json (map) column
    val jsonColumnData = rawData.select(
      $"offset",
      from_json($"json_str", schema=jsonSchema).alias("data")
    )

    // change the map column to multi-column
    // without alias the column name is data.id
    val jsonData = jsonColumnData.select(
      $"offset",
      $"data.id".alias("id"),
      $"data.time".alias("time"),
      $"data.value".alias("value")
    )

    // ========================
    // Sorting is not supported on streaming DataFrames/Datasets,
    // unless it is on aggregated DataFrame/DataSet in Complete output mode
    // ========================

    // ========================
    // 如果没有 aggregation 操作，output mode 就只能是 update 或 append，不能是 complete，不然会报下面的错
    // Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;
    // ========================

    // ========================
    // 使用 aggregation 操作，性能慢了许多
    // ========================
    val groupedWindowData = jsonData
      .withWatermark("time", "20 minutes")
      .groupBy(
        window($"time", "15 minutes", "10 minutes"),
        $"id"
      )
      .avg("value")
      .withColumnRenamed("avg(value)", "avg_value")

    if (sink == "Kafka") {
      // ============================
      // scala 的函数或是 if 等语句的最后一个语句的结果作为返回值
      // ============================
      val outputDataSet =
        if (outputFormatIsJson) {
          groupedWindowData.select(
            to_json(
              struct(
                $"window".cast(StringType).alias("window"),
                $"id",
                $"avg_value"
              )
            ).alias("value")
          )
        } else {
          // concat_ws concatenates multiple input string columns together into a single string column,
          // using the given separator.
          groupedWindowData.select(
            concat_ws(
              "##",
              $"window".cast(StringType),
              $"id",
              $"avg_value"
            )
            .cast(StringType)
            .alias("value")
          )
        }

      // Checkpoint location:
      //   For some output sinks where the end-to-end fault-tolerance can be guaranteed,
      //   specify the location where the system will write all the checkpoint information.
      //   This should be a directory in an HDFS-compatible fault-tolerant file system.

      // outputDataSet must contains columns key(optional), value(required), topic(optional)
      // otherwise it can't be sent to Kafka
      val query = outputDataSet
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "spark-test-output")
        .option("checkpointLocation", "checkpoint")
        .outputMode("update")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .start()

      query.awaitTermination()

      /*
      Kafka topic spark-test-output receive messages
      ==============================================
        {"window":"[2020-01-01 08:50:00, 2020-01-01 09:05:00]","id":"id-002","avg_value":37.0}
        {"window":"[2020-01-01 08:50:00, 2020-01-01 09:05:00]","id":"id-003","avg_value":38.0}
        {"window":"[2020-01-01 08:40:00, 2020-01-01 08:55:00]","id":"id-003","avg_value":33.0}
        {"window":"[2020-01-01 08:40:00, 2020-01-01 08:55:00]","id":"id-001","avg_value":31.0}
        {"window":"[2020-01-01 08:40:00, 2020-01-01 08:55:00]","id":"id-002","avg_value":32.0}
        {"window":"[2020-01-01 08:20:00, 2020-01-01 08:35:00]","id":"id-003","avg_value":23.0}
        {"window":"[2020-01-01 08:00:00, 2020-01-01 08:15:00]","id":"id-001","avg_value":11.0}
        {"window":"[2020-01-01 08:00:00, 2020-01-01 08:15:00]","id":"id-002","avg_value":12.0}
        {"window":"[2020-01-01 09:00:00, 2020-01-01 09:15:00]","id":"id-001","avg_value":41.0}
        {"window":"[2020-01-01 08:20:00, 2020-01-01 08:35:00]","id":"id-002","avg_value":22.0}
        {"window":"[2020-01-01 08:00:00, 2020-01-01 08:15:00]","id":"id-003","avg_value":13.0}
        {"window":"[2020-01-01 07:50:00, 2020-01-01 08:05:00]","id":"id-001","avg_value":11.0}
        {"window":"[2020-01-01 09:00:00, 2020-01-01 09:15:00]","id":"id-002","avg_value":42.0}
        {"window":"[2020-01-01 08:20:00, 2020-01-01 08:35:00]","id":"id-001","avg_value":21.0}
        {"window":"[2020-01-01 07:50:00, 2020-01-01 08:05:00]","id":"id-003","avg_value":13.0}
        {"window":"[2020-01-01 08:50:00, 2020-01-01 09:05:00]","id":"id-001","avg_value":36.0}
        {"window":"[2020-01-01 07:50:00, 2020-01-01 08:05:00]","id":"id-002","avg_value":12.0}
        {"window":"[2020-01-01 09:00:00, 2020-01-01 09:15:00]","id":"id-003","avg_value":43.0}
      or
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
      */
    } else {
      val query =
        if (defineForeachFunction) {
          groupedWindowData
            .writeStream
            .foreachBatch(foreachBatchFunction _)
            .outputMode("complete")
            .trigger(Trigger.ProcessingTime("10 seconds"))
            .start()
        } else {
          groupedWindowData
            .writeStream
            .foreachBatch(
              (output: Dataset[_], batchId: Long) => {
                println("\n\n====== " + batchId + " ======\n\n")
                output.orderBy("window", "id").show(100, truncate = false)
              }
            )
            .outputMode("complete")
            .trigger(Trigger.ProcessingTime("10 seconds"))
            .start()
        }
      query.awaitTermination()
    }
  }

  def foreachBatchFunction(batchDS: Dataset[_], epochId: Long): Unit = {
    print("\n\n----- " + epochId + " -----\n\n")

    batchDS.orderBy("window", "id").show(100, truncate = false)
    /*
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
    */
  }
}
