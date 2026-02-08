package mini.spark.scheduler

trait TaskScheduler extends Serializable {
  def runTasks[T](tasks: Seq[Task[T]]): Seq[T]
}
