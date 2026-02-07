package mini.spark

import mini.spark.rdd.{ParallelCollectionRDD, RDD}
import scala.reflect.ClassTag

class SparkContext {
  def parallelize[T](seq: Seq[T], numSlices: Int = SparkContext.DefaultParallelism): RDD[T] = {
    val slices = if (numSlices <= 0) 1 else numSlices
    new ParallelCollectionRDD[T](this, seq, slices)
  }

  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    val parts = rdd.partitions
    val results = new Array[U](parts.length)
    var i = 0
    while (i < parts.length) {
      val part = parts(i)
      val context = TaskContext(part.index)
      results(i) = func(rdd.iterator(part, context))
      i += 1
    }
    results
  }
}

object SparkContext {
  val DefaultParallelism: Int = math.max(1, Runtime.getRuntime.availableProcessors())
}
