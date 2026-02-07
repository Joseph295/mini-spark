package mini.spark.examples

import mini.spark.SparkContext

object WordCountLocal {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext

    val lines = Seq(
      "hello spark",
      "hello mini spark",
      "spark makes rdd simple"
    )

    val words = sc
      .parallelize(lines, numSlices = 2)
      .flatMap(_.split("\\s+"))
      .map(word => (word, 1))

    val pairs = words.collect()
    val counts = pairs
      .groupBy(_._1)
      .map { case (word, items) => (word, items.map(_._2).sum) }
      .toSeq
      .sortBy(_._1)

    println("WordCountLocal result:")
    counts.foreach { case (word, count) => println(s"$word\t$count") }

    // TODO (Sprint 1): replace local aggregation with reduceByKey using shuffle
  }
}
