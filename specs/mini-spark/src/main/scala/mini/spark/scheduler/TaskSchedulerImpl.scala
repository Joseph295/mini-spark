package mini.spark.scheduler

import mini.spark.TaskContext
import mini.spark.storage.BlockManager

class TaskSchedulerImpl(maxTaskAttempts: Int, blockManager: BlockManager) extends TaskScheduler {
  override def runTasks[T](tasks: Seq[Task[T]]): Seq[T] = {
    tasks.map { task =>
      var attemptId = 0
      var done = false
      var result: Option[T] = None
      var lastError: Throwable = null

      while (!done && attemptId < maxTaskAttempts) {
        val context = TaskContext(stageId = task.stageId, partitionId = task.partitionId, attemptId = attemptId)
        try {
          val res = task.run(context)
          blockManager.commitTask(context)
          result = Some(res)
          done = true
        } catch {
          case t: Throwable =>
            blockManager.abortTask(context)
            lastError = t
            attemptId += 1
        }
      }

      result.getOrElse {
        throw new RuntimeException(
          s"Task failed after $maxTaskAttempts attempts (stage=${task.stageId}, partition=${task.partitionId})",
          lastError
        )
      }
    }
  }
}
