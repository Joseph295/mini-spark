package mini.spark.scheduler

import mini.spark.{Partition, ShuffleDependency, TaskContext}
import mini.spark.rdd.RDD

class ShuffleMapTask[K, V](
    stageId: Int,
    partitionId: Int,
    rdd: RDD[(K, V)],
    shuffleDep: ShuffleDependency[K, V]
) extends Task[Unit](stageId, partitionId) {
  override def run(context: TaskContext): Unit = {
    val part: Partition = rdd.partitions(partitionId)
    val iter = rdd.iterator(part, context)
    val writer = rdd.sc.shuffleManager.getWriter[K, V](shuffleDep.shuffleHandle, partitionId)
    writer.write(iter, context.attemptId)
  }
}
