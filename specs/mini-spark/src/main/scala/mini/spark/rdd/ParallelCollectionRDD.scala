package mini.spark.rdd

import mini.spark.{Partition, SparkContext, TaskContext}

private case class ParallelCollectionPartition[T](index: Int, data: Seq[T]) extends Partition

class ParallelCollectionRDD[T](sc: SparkContext, data: Seq[T], numSlices: Int) extends RDD[T](sc) {
  private val slices: Array[Seq[T]] = ParallelCollectionRDD.slice(data, numSlices)
  private val parts: Array[Partition] = slices.zipWithIndex.map {
    case (slice, idx) => ParallelCollectionPartition(idx, slice): Partition
  }

  override def partitions: Array[Partition] = parts

  override protected def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    val p = partition.asInstanceOf[ParallelCollectionPartition[T]]
    p.data.iterator
  }
}

object ParallelCollectionRDD {
  def slice[T](data: Seq[T], numSlices: Int): Array[Seq[T]] = {
    val total = data.size
    val slices = if (numSlices <= 0) 1 else numSlices
    val step = math.max(1, math.ceil(total.toDouble / slices).toInt)

    Array.tabulate(slices) { i =>
      val start = i * step
      val end = math.min(start + step, total)
      if (start >= total) Seq.empty[T] else data.slice(start, end)
    }
  }
}
