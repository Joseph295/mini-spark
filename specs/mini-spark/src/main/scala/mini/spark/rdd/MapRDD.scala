package mini.spark.rdd

import mini.spark.{Dependency, NarrowDependency, Partition, TaskContext}

class MapRDD[T, U](prev: RDD[T], f: T => U) extends RDD[U](prev.sc) {
  override def partitions: Array[Partition] = prev.partitions

  override def dependencies: Seq[Dependency[_]] = Seq(new NarrowDependency(prev))

  override protected def compute(partition: Partition, context: TaskContext): Iterator[U] = {
    prev.iterator(partition, context).map(f)
  }
}
