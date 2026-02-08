package mini.spark.scheduler

import mini.spark.TaskContext

class TaskSchedulerImpl extends TaskScheduler {
  override def runTasks[T](tasks: Seq[Task[T]]): Seq[T] = {
    tasks.map { task =>
      val context = TaskContext(stageId = task.stageId, partitionId = task.partitionId, attemptNumber = 0)
      task.run(context)
    }
  }
}
