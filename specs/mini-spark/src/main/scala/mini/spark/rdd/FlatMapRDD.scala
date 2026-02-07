package mini.spark.rdd

import mini.spark.{Partition, TaskContext}

class FlatMapRDD[T, U](prev: RDD[T], f: T => TraversableOnce[U]) extends RDD[U](prev.sc) {
  override def partitions: Array[Partition] = prev.partitions

  override private[spark] def dependencies: Seq[RDD[_]] = Seq(prev)

  override protected def compute(partition: Partition, context: TaskContext): Iterator[U] = {
    prev.iterator(partition, context).flatMap(f)
  }
}
