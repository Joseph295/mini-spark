package mini.spark.rdd

import mini.spark.{Dependency, NarrowDependency, Partition, TaskContext}

class FilterRDD[T](prev: RDD[T], f: T => Boolean) extends RDD[T](prev.sc) {
  override def partitions: Array[Partition] = prev.partitions

  override def dependencies: Seq[Dependency[_]] = Seq(new NarrowDependency(prev))

  override protected def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    prev.iterator(partition, context).filter(f)
  }
}
