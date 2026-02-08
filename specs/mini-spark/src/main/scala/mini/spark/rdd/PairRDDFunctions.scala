package mini.spark.rdd

import mini.spark.{HashPartitioner, Partitioner}

class PairRDDFunctions[K, V](rdd: RDD[(K, V)]) extends Serializable {
  def reduceByKey(func: (V, V) => V, numPartitions: Int = rdd.partitions.length): RDD[(K, V)] = {
    val partitions = if (numPartitions <= 0) 1 else numPartitions
    val partitioner: Partitioner = new HashPartitioner(partitions)
    new ShuffledRDD[K, V](rdd, partitioner, func)
  }
}
