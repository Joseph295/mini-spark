package mini.spark.scheduler

import mini.spark.{Partition, TaskContext}
import mini.spark.rdd.RDD

class ResultTask[T, U](
    stageId: Int,
    partitionId: Int,
    rdd: RDD[T],
    func: Iterator[T] => U
) extends Task[U](stageId, partitionId) {
  override def run(context: TaskContext): U = {
    val part: Partition = rdd.partitions(partitionId)
    func(rdd.iterator(part, context))
  }
}
