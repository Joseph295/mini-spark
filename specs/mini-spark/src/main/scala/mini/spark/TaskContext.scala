package mini.spark

case class TaskContext(stageId: Int, partitionId: Int, attemptId: Int)

object TaskContext {
  def apply(partitionId: Int): TaskContext = TaskContext(stageId = 0, partitionId = partitionId, attemptId = 0)
}
