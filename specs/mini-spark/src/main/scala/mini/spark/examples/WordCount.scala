package mini.spark.examples

import mini.spark.{NarrowDependency, SparkContext}
import mini.spark.rdd.RDD._
import mini.spark.storage.StorageLevel

object WordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext

    val lines = Seq(
      "hello spark",
      "hello mini spark",
      "spark makes rdd simple"
    )

    val base = sc
      .parallelize(lines, numSlices = 2)
      .flatMap(_.split("\\s+"))
      .persist(StorageLevel.MEMORY_ONLY)

    val maybeFailing = if (args.contains("--fail-once")) {
      new FailOnceRDD[String](base)
    } else {
      base
    }

    val counts = maybeFailing
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect()
      .toSeq
      .sortBy(_._1)

    println("WordCount result:")
    counts.foreach { case (word, count) => println(s"$word\t$count") }

    val totalWords = maybeFailing.count()
    println(s"Total words: $totalWords")
  }
}

private class FailOnceRDD[T](prev: mini.spark.rdd.RDD[T]) extends mini.spark.rdd.RDD[T](prev.sc) {
  override def partitions: Array[mini.spark.Partition] = prev.partitions

  override def dependencies: Seq[mini.spark.Dependency[_]] = Seq(new NarrowDependency(prev))

  override protected def compute(partition: mini.spark.Partition, context: mini.spark.TaskContext): Iterator[T] = {
    if (partition.index == 0 && context.attemptId == 0) {
      throw new RuntimeException("Injected failure for retry demo")
    }
    prev.iterator(partition, context)
  }
}
