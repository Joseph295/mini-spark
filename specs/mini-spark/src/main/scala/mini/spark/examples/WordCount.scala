package mini.spark.examples

import mini.spark.SparkContext
import mini.spark.rdd.RDD._

object WordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext

    val lines = Seq(
      "hello spark",
      "hello mini spark",
      "spark makes rdd simple"
    )

    val counts = sc
      .parallelize(lines, numSlices = 2)
      .flatMap(_.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect()
      .toSeq
      .sortBy(_._1)

    println("WordCount result:")
    counts.foreach { case (word, count) => println(s"$word\t$count") }
  }
}
