package mini.spark.rdd

import mini.spark.{Dependency, Partition, Partitioner, ShuffleDependency, TaskContext}
import scala.collection.mutable

private case class ShuffledRDDPartition(index: Int) extends Partition

class ShuffledRDD[K, V](
    prev: RDD[(K, V)],
    partitioner: Partitioner,
    reduceFunc: (V, V) => V
) extends RDD[(K, V)](prev.sc) {
  private val shuffleDep = new ShuffleDependency[K, V](prev, partitioner)

  override def dependencies: Seq[Dependency[_]] = Seq(shuffleDep)

  override def partitions: Array[Partition] =
    Array.tabulate(partitioner.numPartitions)(i => ShuffledRDDPartition(i))

  override protected def compute(partition: Partition, context: TaskContext): Iterator[(K, V)] = {
    val reduceId = partition.index
    val reader = sc.shuffleManager.getReader[K, V](shuffleDep.shuffleHandle, reduceId)
    val it = reader.read()
    val map = mutable.HashMap.empty[K, V]
    it.foreach { case (k, v) =>
      map.update(k, map.get(k).map(existing => reduceFunc(existing, v)).getOrElse(v))
    }
    map.iterator
  }
}
