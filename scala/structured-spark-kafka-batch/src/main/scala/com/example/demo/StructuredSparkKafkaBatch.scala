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
                 --class com.example.demo.StructuredSparkKafkaBatch \
                 structured-spark-kafka-batch_2.11-0.1.jar

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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat_ws, from_json, get_json_object, json_tuple, struct, to_json, window}
import org.apache.spark.sql.types._


object StructuredSparkKafkaBatch {

  // ==================================
  // case class 要定义在引用 case class 的函数 (这个例子里是 main 函数) 的外面
  // ==================================
  case class Data_1(id: String, name: String, value: Int)
  case class Data_2(key: String, value: String)

  def main(args: Array[String]): Unit = {

    // INFO 日志的量非常大，改成 WARN level 方便查看运行结果
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val printDataSet = true
    val outputFormatIsJson = true

    val spark = SparkSession.builder
                            .appName("StructuredSparkKafkaBatch")
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

    var rawData = spark.read
                       .format("kafka")
                       .option("kafka.bootstrap.servers", "localhost:9092")
                       .option("subscribe", "spark-test")   // or .option("assign", """{"spark-test":[0]}""")
                       .option("startingOffsets", "earliest")
                       .option("endingOffsets", "latest")
                       .load()
                       .cache()

    if (printDataSet) {
      /*
      +----+------------------+----------+---------+------+-----------------------+-------------+
      |key |value             |topic     |partition|offset|timestamp              |timestampType|
      +----+------------------+----------+---------+------+-----------------------+-------------+
      |null|[7B 22 69 ... ...]|spark-test|0        |0     |2021-03-04 01:26:23.068|0            |
      |null|[7B 22 69 ... ...]|spark-test|0        |1     |2021-03-04 01:26:23.078|0            |
      */
      rawData.show(100, truncate = false)

      // class org.apache.spark.sql.Dataset
      println(rawData.getClass)
    }

    // selectExpr projects a set of SQL expressions and returns a new Dataset/DataFrame, e.g.
    //     data_set.selectExpr("column_1/2 as new_column_1", "round(column_2/column_1) as new_column_2")
    rawData = rawData.selectExpr("offset", "timestamp", "CAST(value AS STRING) as json_str").cache()

    // explode can convert a map/array column to multi rows
    import org.apache.spark.sql.functions.{explode, split}
    val ds_2 = Seq(Data_2("ID_1", "A,B,C"),
                   Data_2("ID_2", "D,E,F")).toDS()
    ds_2.select($"key", explode(split($"value", ",")).alias("value")).show()
    /*
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
    */

    val jsonSchema = new StructType(
      Array(
        StructField("id", StringType),
        StructField("time", TimestampType, nullable = true),
        StructField("value", IntegerType, nullable = true)
      )
    )

    // from_json parse a string column to a json (map) column
    val jsonColumnData_1 = rawData.select(
      $"offset",
      from_json($"json_str", schema=jsonSchema).alias("data")
    ).cache()

    // change the map column to multi-column
    // without alias the column name is data.id
    val jsonData_1 = jsonColumnData_1.select(
      $"offset",
      $"data.id".alias("id"),
      $"data.time".alias("time"),
      $"data.value".alias("value")
    )

    if (printDataSet) {
      /*
      +------+---------------------------------+
      |offset|data                             |
      +------+---------------------------------+
      |0     |[id-001, 2020-01-01 08:00:01, 11]|
      |1     |[id-002, 2020-01-01 08:00:02, 12]|
      |2     |[id-003, 2020-01-01 08:00:03, 13]|
      */
      jsonColumnData_1.show(100, truncate = false)

      /*
      +------+------+-------------------+-----+
      |offset|id    |time               |value|
      +------+------+-------------------+-----+
      |0     |id-001|2020-01-01 08:00:01|11   |
      |1     |id-002|2020-01-01 08:00:02|12   |
      |2     |id-003|2020-01-01 08:00:03|13   |
      */
      jsonData_1.show(100, truncate = false)
    }

    // from_json also can parse the nested json data
    val ds_3 = Seq(Data_2("1", """{"f1": "value1", "f2": {"f3": "value2"}}"""),
                   Data_2("2", """{"f1": "value12"}""")).toDS()
    val jsonSchema_3 = new StructType(
      Array(
        StructField("f1", StringType),
        StructField("f2", StructType(Array(StructField("f3", StringType))), nullable = true)
      )
    )
    val jsonColumnData_3 = ds_3.select(
      $"key",
      from_json($"value", schema=jsonSchema_3).alias("data")
    )
    val jsonData_3 = jsonColumnData_3.select(
      $"data.f1".alias("f1"),
      $"data.f2.f3".alias("f3")
    )
    jsonData_3.show(100, truncate = false)
    /*
    +-------+------+
    |f1     |f3    |
    +-------+------+
    |value1 |value2|
    |value12|null  |
    +-------+------+
    */

    // json_tuple parse a json string column to multi-column
    // (without as(), the column name will be c0, c1, c2, ...)
    val jsonData_4 = rawData.select(
      $"offset",
      json_tuple($"json_str", "id", "time", "value").as(Seq("id", "time", "value"))
    )
    if (printDataSet) {
      /*
      +------+------+-------------------+-----+
      |offset|id    |time               |value|
      +------+------+-------------------+-----+
      |0     |id-001|2020-01-01 08:00:01|11   |
      |1     |id-002|2020-01-01 08:00:02|12   |
      |2     |id-003|2020-01-01 08:00:03|13   |
      */
      jsonData_4.show(100, truncate = false)
    }

    // get_json_object create a column base on the json column and json path
    val jsonData_5 = rawData.select(
      $"offset",
      get_json_object($"json_str", "$.id").alias("id"),
      get_json_object($"json_str", "$.time").alias("time"),
      get_json_object($"json_str", "$.value").alias("value").cast(IntegerType)
    ).cache()
    if (printDataSet) {
      /*
      +------+------+-------------------+-----+
      |offset|id    |time               |value|
      +------+------+-------------------+-----+
      |0     |id-001|2020-01-01 08:00:01|11   |
      |1     |id-002|2020-01-01 08:00:02|12   |
      |2     |id-003|2020-01-01 08:00:03|13   |
      */
      jsonData_5.show(100, truncate = false)
    }

    // get_json_object also can parse the nested json data
    val ds_6 = Seq(Data_2("1", """{"f1": "value1", "f2": {"f3": "value2"}}"""),
                   Data_2("2", """{"f1": "value12"}""")).toDS()
    ds_6.select(
      $"key",
      get_json_object($"value", "$.f1").alias("c1"),
      get_json_object($"value", "$.f2.f3").alias("c2")
    ).show()
    /*
    +---+-------+------+
    |key|     c1|    c2|
    +---+-------+------+
    |  1| value1|value2|
    |  2|value12|  null|
    +---+-------+------+
    */

    if (printDataSet) {
      /*
      +------+------+-------------------+-----+
      |offset|id    |time               |value|
      +------+------+-------------------+-----+
      |3     |id-001|2020-01-01 08:25:01|21   |
      |4     |id-002|2020-01-01 08:25:02|22   |
      */
      jsonData_5.select("*").where("value > 20")
        .show(100, truncate = false)

      /*
      +------+------+-----+
      |offset|id    |value|
      +------+------+-----+
      |3     |id-001|21   |
      |4     |id-002|22   |
      */
      jsonData_5.select("offset", "id", "value").where("value > 20")
        .show(100, truncate = false)

      /*
      +------+-----+
      |id    |count|
      +------+-----+
      |id-001|3    |
      |id-003|3    |
      |id-002|3    |
      +------+-----+
      */
      jsonData_5.where("value > 20").groupBy("id").count()
        .show(100, truncate = false)

      /*
      +------+----------+
      |id    |avg(value)|
      +------+----------+
      |id-001|31.0      |
      |id-003|33.0      |
      |id-002|32.0      |
      +------+----------+
      */
      jsonData_5.where("value > 20").groupBy("id").avg("value")
        .show(100, truncate = false)

      /*
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
      */
      val groupedWindowData_1 = jsonData_5
        .groupBy(
          window($"time", "15 minutes", "15 minutes"),
          $"id"
        )
        .avg("value")
        .orderBy("window", "id")
        .withColumnRenamed("avg(value)", "avg_value")
        .cache()

      groupedWindowData_1.show(100, truncate = false)

      /*
      +--------------+------------------------------------------+
      |max(avg_value)|min(window)                               |
      +--------------+------------------------------------------+
      |43.0          |[2020-01-01 08:00:00, 2020-01-01 08:15:00]|
      +--------------+------------------------------------------+
      */
      groupedWindowData_1.agg("avg_value" -> "max", "window" -> "min")
        .show(100, truncate = false)
    }

    val groupedWindowData_2 = jsonData_5
      .groupBy(
        window($"time", "15 minutes", "15 minutes"),
        $"id"
      )
      .avg("value")
      .orderBy("window", "id")
      .withColumnRenamed("avg(value)", "avg_value")

    // ======================
    // scala 的函数或是 if 等语句的最后一个语句的结果作为返回值
    // ======================
    val outputDataSet =
      if (outputFormatIsJson) {
        groupedWindowData_2.select(
          to_json(
            struct(
              $"window".cast(StringType).alias("window"),
              $"id",
              $"avg_value"
            )
          ).alias("value")
        ).cache()
        /*
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
        */
      } else {
        // concat_ws concatenates multiple input string columns together into a single string column,
        // using the given separator.
        val outputDataSet_temp = groupedWindowData_2.select(
          concat_ws(
            "##",
            $"window".cast(StringType),
            $"id",
            $"avg_value"
          )
          .cast(StringType)
          .alias("value")
        ).cache()

        outputDataSet_temp
        /*
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
        */
      }

    outputDataSet.show(100, truncate = false)

    // outputDataSet must contains columns key(optional), value(required), topic(optional)
    // otherwise it can't be sent to Kafka
    outputDataSet
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "spark-test-output")
      .save()
  }
}
