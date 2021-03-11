import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

object WordCount {
  def main(args: Array[String]) {
    //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val inputFile = "./src/main/scala/WordCount.scala"
    val textFile = sc.textFile(inputFile)

    // don't need to escape }, ], /
    // regexp should be \s, but in order to escape \, have to change to \\s
    val regex = "\\s+|\\.|\\(|\\)|\\{|}|\\[|]|/|\\\\|\"|\\||,|:"
    val wordCount = textFile.flatMap(line => line.split(regex))
                            .filter(word => word != "")
                            .map(word => (word, 1))
                            .reduceByKey((x, y) => x + y)

    wordCount.foreach(println)
  }
}
