package com.example.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._


/**
 * Kafka Data
 *
 * {"id":"id-001", "time":"2020-01-01 08:00:01", "value":11}
 * {"id":"id-002", "time":"2020-01-01 08:00:02", "value":12}
 * {"id":"id-003", "time":"2020-01-01 08:00:03", "value":13}
 * {"id":"id-001", "time":"2020-01-01 08:25:01", "value":21}
 * {"id":"id-002", "time":"2020-01-01 08:25:02", "value":22}
 * {"id":"id-003", "time":"2020-01-01 08:25:03", "value":23}
 * {"id":"id-001", "time":"2020-01-01 08:50:01", "value":31}
 * {"id":"id-002", "time":"2020-01-01 08:50:02", "value":32}
 * {"id":"id-003", "time":"2020-01-01 08:50:03", "value":33}
 * {"id":"id-001", "time":"2020-01-01 09:00:01", "value":41}
 * {"id":"id-002", "time":"2020-01-01 09:00:02", "value":42}
 * {"id":"id-003", "time":"2020-01-01 09:00:03", "value":43}
 */
object StructuredSparkStreamingJoinStatic {

  // ==================================
  // case class 要定义在引用 case class 的函数 (这个例子里是 main 函数) 的外面
  // ==================================
  case class User(userId: String, name: String, category: String)
  
  val kafkaJsonSchema = new StructType(
      Array(
        StructField("id", StringType),
        StructField("time", TimestampType, nullable = true),
        StructField("value", IntegerType, nullable = true)
      )
    )

  def main(args: Array[String]): Unit = {
  
    // INFO 日志的量非常大，改成 WARN level 方便查看运行结果
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder
                            .appName("StructuredSparkStreamingJoinStatic")
                            .master("local[2]")
                            .getOrCreate()

    // ====================================
    // Seq 不是 spark 的类，而是 Scala 的类
    // 必须导入 spark 的 implicits (隐式) 才能使 Seq 有 toDS 函数可以转成 DataSet
    // 同时才可以在 select 函数中用 $"name" 表达 Column 类
    // ====================================
    import spark.implicits._

    val userData = Seq(User("id-002", "name_b", "type_b"),
                       User("id-003", "name_c", "type_c"),
                       User("id-004", "name_d", "type_d")).toDS().cache()
    userData.show(100, truncate = false)

    val kafkaData = spark.readStream
                         .format("kafka")
                         .option("kafka.bootstrap.servers", "localhost:9092")
                         .option("subscribe", "spark-test")        // or .option("assign", """{"spark-test":[0]}""")
                         .option("startingOffsets", "earliest")
                         .load()
                         .selectExpr("offset", "timestamp as kafka_time", "CAST(value AS STRING) as json_str")
                         .select(
                            $"offset",
                            $"kafka_time",
                            from_json($"json_str", schema=kafkaJsonSchema).alias("data")
                         )
                         .select(
                            $"offset",
                            $"kafka_time",
                            $"data.id".alias("userId"),
                            $"data.time".alias("data_time"),
                            $"data.value".alias("value")
                         )

    // === 是 spark Column 类定义的函数，用于比较两个列
    // 这种方法不会去重，即结果会有两个 userId 列
    // 多个比较条件的话用 ds_1("col_1") === ds_2("col_1") && ds_1("col_2") === ds_2("col_2")
    val joinedDF_1 = kafkaData.join(userData, kafkaData("userId") === userData("userId"))

    // 这种方法会去重，但要求列名一样
    // 多个比较条件的话用 Seq("col_1", "col_2")
    val joinedDF_2 = kafkaData.join(userData, Seq("userId"))

    // 指定 joinType，默认是 inner
    val joinedDF_3 = kafkaData.join(userData, Seq("userId"), "left")

    /**
     * +------+-----------------------+------+-------------------+-----+------+------+--------+
     * |offset|kafka_time             |userId|data_time          |value|userId|name  |category|
     * +------+-----------------------+------+-------------------+-----+------+------+--------+
     * |1     |2021-03-11 22:49:31.107|id-002|2020-01-01 08:00:02|12   |id-002|name_b|type_b  |
     * |2     |2021-03-11 22:49:31.108|id-003|2020-01-01 08:00:03|13   |id-003|name_c|type_c  |
     * |4     |2021-03-11 22:49:31.108|id-002|2020-01-01 08:25:02|22   |id-002|name_b|type_b  |
     * |5     |2021-03-11 22:49:31.108|id-003|2020-01-01 08:25:03|23   |id-003|name_c|type_c  |
     * |7     |2021-03-11 22:49:31.108|id-002|2020-01-01 08:50:02|32   |id-002|name_b|type_b  |
     * |8     |2021-03-11 22:49:31.108|id-003|2020-01-01 08:50:03|33   |id-003|name_c|type_c  |
     * |10    |2021-03-11 22:49:31.109|id-002|2020-01-01 09:00:02|42   |id-002|name_b|type_b  |
     * |11    |2021-03-11 22:49:31.96 |id-003|2020-01-01 09:00:03|43   |id-003|name_c|type_c  |
     * +------+-----------------------+------+-------------------+-----+------+------+--------+
     */
    joinedDF_1.writeStream
              .outputMode("append")
              .trigger(Trigger.ProcessingTime("2 seconds"))
              .format("console")
              .option("truncate", "false")
              .start()

    /**
     * +------+------+-----------------------+-------------------+-----+------+--------+
     * |userId|offset|kafka_time             |data_time          |value|name  |category|
     * +------+------+-----------------------+-------------------+-----+------+--------+
     * |id-002|1     |2021-03-11 22:49:31.107|2020-01-01 08:00:02|12   |name_b|type_b  |
     * |id-003|2     |2021-03-11 22:49:31.108|2020-01-01 08:00:03|13   |name_c|type_c  |
     * |id-002|4     |2021-03-11 22:49:31.108|2020-01-01 08:25:02|22   |name_b|type_b  |
     * |id-003|5     |2021-03-11 22:49:31.108|2020-01-01 08:25:03|23   |name_c|type_c  |
     * |id-002|7     |2021-03-11 22:49:31.108|2020-01-01 08:50:02|32   |name_b|type_b  |
     * |id-003|8     |2021-03-11 22:49:31.108|2020-01-01 08:50:03|33   |name_c|type_c  |
     * |id-002|10    |2021-03-11 22:49:31.109|2020-01-01 09:00:02|42   |name_b|type_b  |
     * |id-003|11    |2021-03-11 22:49:31.96 |2020-01-01 09:00:03|43   |name_c|type_c  |
     * +------+------+-----------------------+-------------------+-----+------+--------+
     */
    joinedDF_2.writeStream
              .outputMode("append")
              .trigger(Trigger.ProcessingTime("2 seconds"))
              .format("console")
              .option("truncate", "false")
              .start()

    /**
     * +------+------+-----------------------+-------------------+-----+------+--------+
     * |userId|offset|kafka_time             |data_time          |value|name  |category|
     * +------+------+-----------------------+-------------------+-----+------+--------+
     * |id-001|0     |2021-03-11 22:49:31.1  |2020-01-01 08:00:01|11   |null  |null    |
     * |id-002|1     |2021-03-11 22:49:31.107|2020-01-01 08:00:02|12   |name_b|type_b  |
     * |id-003|2     |2021-03-11 22:49:31.108|2020-01-01 08:00:03|13   |name_c|type_c  |
     * |id-001|3     |2021-03-11 22:49:31.108|2020-01-01 08:25:01|21   |null  |null    |
     * |id-002|4     |2021-03-11 22:49:31.108|2020-01-01 08:25:02|22   |name_b|type_b  |
     * |id-003|5     |2021-03-11 22:49:31.108|2020-01-01 08:25:03|23   |name_c|type_c  |
     * |id-001|6     |2021-03-11 22:49:31.108|2020-01-01 08:50:01|31   |null  |null    |
     * |id-002|7     |2021-03-11 22:49:31.108|2020-01-01 08:50:02|32   |name_b|type_b  |
     * |id-003|8     |2021-03-11 22:49:31.108|2020-01-01 08:50:03|33   |name_c|type_c  |
     * |id-001|9     |2021-03-11 22:49:31.108|2020-01-01 09:00:01|41   |null  |null    |
     * |id-002|10    |2021-03-11 22:49:31.109|2020-01-01 09:00:02|42   |name_b|type_b  |
     * |id-003|11    |2021-03-11 22:49:31.96 |2020-01-01 09:00:03|43   |name_c|type_c  |
     * +------+------+-----------------------+-------------------+-----+------+--------+
     */
    joinedDF_3.writeStream
              .outputMode("append")
              .trigger(Trigger.ProcessingTime("2 seconds"))
              .format("console")
              .option("truncate", "false")
              .start()

    // 可以同时执行多个 stream
    spark.streams.awaitAnyTermination()
  }
}
