package mini.spark.rdd

import mini.spark.{Partition, TaskContext}

class FilterRDD[T](prev: RDD[T], f: T => Boolean) extends RDD[T](prev.sc) {
  override def partitions: Array[Partition] = prev.partitions

  override private[spark] def dependencies: Seq[RDD[_]] = Seq(prev)

  override protected def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    prev.iterator(partition, context).filter(f)
  }
}
