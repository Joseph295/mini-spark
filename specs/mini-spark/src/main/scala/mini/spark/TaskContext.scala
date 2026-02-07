package mini.spark

case class TaskContext(stageId: Int, partitionId: Int, attemptNumber: Int)

object TaskContext {
  def apply(partitionId: Int): TaskContext = TaskContext(stageId = 0, partitionId = partitionId, attemptNumber = 0)
}
