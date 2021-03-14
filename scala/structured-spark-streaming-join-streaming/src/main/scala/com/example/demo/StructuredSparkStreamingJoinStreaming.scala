package com.example.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.{from_json, expr}
import org.apache.spark.sql.types._


/**
 * Kafka-1 Data
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
 *
 * Kafka-2 Data
 *
 * {"id":"id-001", "time":"2020-01-01 08:00:11", "event":"e-001"}
 * {"id":"id-002", "time":"2020-01-01 08:00:12", "event":"e-002"}
 * {"id":"id-003", "time":"2020-01-01 08:00:33", "event":"e-003"}
 * {"id":"id-001", "time":"2020-01-01 08:25:11", "event":"e-001"}
 * {"id":"id-002", "time":"2020-01-01 08:25:12", "event":"e-002"}
 * {"id":"id-003", "time":"2020-01-01 08:25:33", "event":"e-003"}
 * {"id":"id-001", "time":"2020-01-01 08:50:11", "event":"e-001"}
 * {"id":"id-002", "time":"2020-01-01 08:50:12", "event":"e-002"}
 * {"id":"id-003", "time":"2020-01-01 08:50:33", "event":"e-003"}
 * {"id":"id-001", "time":"2020-01-01 09:00:11", "event":"e-001"}
 * {"id":"id-002", "time":"2020-01-01 09:00:12", "event":"e-002"}
 * {"id":"id-003", "time":"2020-01-01 09:00:33", "event":"e-003"}
 */
object StructuredSparkStreamingJoinStreaming {

  // ==================================
  // case class 要定义在引用 case class 的函数 (这个例子里是 main 函数) 的外面
  // ==================================
  case class User(userId: String, name: String, category: String)
  
  val kafkaJsonSchema_1 = new StructType(
    Array(
      StructField("id", StringType),
      StructField("time", TimestampType, nullable = true),
      StructField("value", IntegerType, nullable = true)
    )
  )

  val kafkaJsonSchema_2 = new StructType(
    Array(
      StructField("id", StringType),
      StructField("time", TimestampType, nullable = true),
      StructField("event", StringType, nullable = true)
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

    val kafkaData_1 = spark.readStream
                           .format("kafka")
                           .option("kafka.bootstrap.servers", "localhost:9092")
                           .option("subscribe", "spark-test")        // or .option("assign", """{"spark-test":[0]}""")
                           .option("startingOffsets", "earliest")
                           .load()
                           .selectExpr(
                              "offset",
                              "timestamp as kafka_time_1",
                              "CAST(value AS STRING) as json_str")
                           .select(
                              $"offset",
                              $"kafka_time_1",
                              from_json($"json_str", schema=kafkaJsonSchema_1).alias("data")
                           )
                           .select(
                              $"offset".alias("offset_1"),
                              $"kafka_time_1",
                              $"data.id".alias("userId"),
                              $"data.time".alias("data_time_1"),
                              $"data.value".alias("value")
                           )
                           .filter($"data_time_1".isNotNull && $"userId".isNotNull)
                           .withWatermark("data_time_1", "20 minutes")
                           
    val kafkaData_2 = spark.readStream
                           .format("kafka")
                           .option("kafka.bootstrap.servers", "localhost:9092")
                           .option("subscribe", "spark-test-3")      // or .option("assign", """{"spark-test-3":[0]}""")
                           .option("startingOffsets", "earliest")
                           .load()
                           .selectExpr(
                              "offset",
                              "timestamp as kafka_time_2",
                              "CAST(value AS STRING) as json_str")
                           .select(
                              $"offset",
                              $"kafka_time_2",
                              from_json($"json_str", schema=kafkaJsonSchema_2).alias("data")
                           )
                           .select(
                              $"offset".alias("offset_2"),
                              $"kafka_time_2",
                              $"data.id".alias("userId"),
                              $"data.time".alias("data_time_2"),
                              $"data.event".alias("event")
                           )
                           .filter($"data_time_2".isNotNull && $"userId".isNotNull)
                           .withWatermark("data_time_2", "20 minutes")

    val joinedStream_1 = kafkaData_1.as("ds_1").join(
      kafkaData_2.as("ds_2"),
      expr(
        """
          ds_1.userId = ds_2.userId AND
          data_time_2 >= data_time_1 AND
          data_time_2 <= data_time_1 + interval 20 seconds
        """
      )
    ).drop($"ds_1.userId")

    val joinedStatic_1 = joinedStream_1.join(userData, Seq("userId"))

    kafkaData_1.writeStream
               .outputMode("append")
               .trigger(Trigger.ProcessingTime("2 seconds"))
               .format("console")
               .option("truncate", "false")
               .start()
               
    kafkaData_2.writeStream
               .outputMode("append")
               .trigger(Trigger.ProcessingTime("2 seconds"))
               .format("console")
               .option("truncate", "false")
               .start()

    /**
     * +--------+-----------------------+-------------------+-----+--------+-----------------------+------+-------------------+-----+
     * |offset_1|kafka_time_1           |data_time_1        |value|offset_2|kafka_time_2           |userId|data_time_2        |event|
     * +--------+-----------------------+-------------------+-----+--------+-----------------------+------+-------------------+-----+
     * |0       |2021-03-11 22:49:31.1  |2020-01-01 08:00:01|11   |0       |2021-03-14 18:08:25.675|id-001|2020-01-01 08:00:11|e-001|
     * |3       |2021-03-11 22:49:31.108|2020-01-01 08:25:01|21   |3       |2021-03-14 18:08:25.785|id-001|2020-01-01 08:25:11|e-001|
     * |6       |2021-03-11 22:49:31.108|2020-01-01 08:50:01|31   |6       |2021-03-14 18:08:25.786|id-001|2020-01-01 08:50:11|e-001|
     * |9       |2021-03-11 22:49:31.108|2020-01-01 09:00:01|41   |9       |2021-03-14 18:08:25.787|id-001|2020-01-01 09:00:11|e-001|
     * |1       |2021-03-11 22:49:31.107|2020-01-01 08:00:02|12   |1       |2021-03-14 18:08:25.785|id-002|2020-01-01 08:00:12|e-002|
     * |4       |2021-03-11 22:49:31.108|2020-01-01 08:25:02|22   |4       |2021-03-14 18:08:25.786|id-002|2020-01-01 08:25:12|e-002|
     * |7       |2021-03-11 22:49:31.108|2020-01-01 08:50:02|32   |7       |2021-03-14 18:08:25.786|id-002|2020-01-01 08:50:12|e-002|
     * |10      |2021-03-11 22:49:31.109|2020-01-01 09:00:02|42   |10      |2021-03-14 18:08:25.787|id-002|2020-01-01 09:00:12|e-002|
     * +--------+-----------------------+-------------------+-----+--------+-----------------------+------+-------------------+-----+
     */
    joinedStream_1.writeStream
                  .outputMode("append")
                  .trigger(Trigger.ProcessingTime("2 seconds"))
                  .format("console")
                  .option("truncate", "false")
                  .start()

    /**
     * +------+--------+-----------------------+-------------------+-----+--------+-----------------------+-------------------+-----+------+--------+
     * |userId|offset_1|kafka_time_1           |data_time_1        |value|offset_2|kafka_time_2           |data_time_2        |event|name  |category|
     * +------+--------+-----------------------+-------------------+-----+--------+-----------------------+-------------------+-----+------+--------+
     * |id-002|1       |2021-03-11 22:49:31.107|2020-01-01 08:00:02|12   |1       |2021-03-14 18:08:25.785|2020-01-01 08:00:12|e-002|name_b|type_b  |
     * |id-002|4       |2021-03-11 22:49:31.108|2020-01-01 08:25:02|22   |4       |2021-03-14 18:08:25.786|2020-01-01 08:25:12|e-002|name_b|type_b  |
     * |id-002|7       |2021-03-11 22:49:31.108|2020-01-01 08:50:02|32   |7       |2021-03-14 18:08:25.786|2020-01-01 08:50:12|e-002|name_b|type_b  |
     * |id-002|10      |2021-03-11 22:49:31.109|2020-01-01 09:00:02|42   |10      |2021-03-14 18:08:25.787|2020-01-01 09:00:12|e-002|name_b|type_b  |
     * +------+--------+-----------------------+-------------------+-----+--------+-----------------------+-------------------+-----+------+--------+
     */
    joinedStatic_1.writeStream
                  .outputMode("append")
                  .trigger(Trigger.ProcessingTime("2 seconds"))
                  .format("console")
                  .option("truncate", "false")
                  .start()

    // 可以同时执行多个 stream
    spark.streams.awaitAnyTermination()
  }
}
