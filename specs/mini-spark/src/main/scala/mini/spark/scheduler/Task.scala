package mini.spark.scheduler

import mini.spark.TaskContext

abstract class Task[T](val stageId: Int, val partitionId: Int) extends Serializable {
  def run(context: TaskContext): T
}
