package mini.spark.scheduler

import java.util.concurrent.atomic.AtomicInteger
import mini.spark.{Dependency, NarrowDependency, ShuffleDependency, SparkContext}
import mini.spark.rdd.RDD
import scala.collection.mutable
import scala.reflect.ClassTag

class DAGScheduler(sc: SparkContext) extends Serializable {
  private val nextStageId = new AtomicInteger(0)
  private val shuffleIdToStage = mutable.HashMap[Int, Stage]()
  private val completedStages = mutable.HashSet[Int]()
  @volatile private var lastJobStages: Seq[Stage] = Seq.empty

  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    completedStages.clear()
    val resultStage = createResultStage(rdd)
    lastJobStages = collectStages(resultStage)
    try {
      val results = runResultStage(resultStage, func)
      results.toArray
    } finally {
      sc.cleanupShuffle()
    }
  }

  def getLastJobStages: Seq[Stage] = lastJobStages

  private def runResultStage[T, U](stage: Stage, func: Iterator[T] => U): Seq[U] = {
    submitParents(stage)
    val rdd = stage.rdd.asInstanceOf[RDD[T]]
    val tasks = rdd.partitions.map { p =>
      new ResultTask[T, U](stage.id, p.index, rdd, func)
    }
    sc.taskScheduler.runTasks(tasks.toSeq)
  }

  private def submitParents(stage: Stage): Unit = {
    stage.parents.foreach(submitStage)
  }

  private def submitStage(stage: Stage): Unit = {
    if (!completedStages.contains(stage.id)) {
      submitParents(stage)
      if (stage.isShuffleMap) {
        runShuffleMapStage(stage)
      }
      completedStages += stage.id
    }
  }

  private def runShuffleMapStage(stage: Stage): Unit = {
    val dep = stage.shuffleDep.get.asInstanceOf[ShuffleDependency[Any, Any]]
    val rdd = stage.rdd.asInstanceOf[RDD[(Any, Any)]]
    val tasks = rdd.partitions.map { p =>
      new ShuffleMapTask[Any, Any](stage.id, p.index, rdd, dep)
    }
    sc.taskScheduler.runTasks(tasks.toSeq)
  }

  private def createResultStage[T](rdd: RDD[T]): Stage = {
    val parents = getShuffleDependencies(rdd).map(getOrCreateShuffleMapStage)
    Stage(nextStageId.getAndIncrement(), rdd, parents, isShuffleMap = false, shuffleDep = None)
  }

  private def getOrCreateShuffleMapStage(dep: ShuffleDependency[_, _]): Stage = {
    shuffleIdToStage.getOrElseUpdate(
      dep.shuffleId, {
        val parents = getShuffleDependencies(dep.rdd).map(getOrCreateShuffleMapStage)
        val numMaps = dep.rdd.partitions.length
        val numReduces = dep.partitioner.numPartitions
        val handle = sc.shuffleManager.registerShuffle(dep.shuffleId, numMaps, numReduces, dep.partitioner)
        dep.setShuffleHandle(handle)
        Stage(nextStageId.getAndIncrement(), dep.rdd, parents, isShuffleMap = true, shuffleDep = Some(dep))
      }
    )
  }

  private def getShuffleDependencies(rdd: RDD[_]): Seq[ShuffleDependency[_, _]] = {
    val shuffleDeps = mutable.ArrayBuffer[ShuffleDependency[_, _]]()

    def visitDependencies(deps: Seq[Dependency[_]]): Unit = {
      deps.foreach {
        case s: ShuffleDependency[_, _] =>
          shuffleDeps += s
        case n: NarrowDependency[_] =>
          visitDependencies(n.rdd.dependencies)
        case other =>
          visitDependencies(other.rdd.dependencies)
      }
    }

    visitDependencies(rdd.dependencies)
    shuffleDeps.toSeq
  }

  private def collectStages(resultStage: Stage): Seq[Stage] = {
    val seen = mutable.HashSet[Int]()
    val ordered = mutable.ArrayBuffer[Stage]()

    def visit(stage: Stage): Unit = {
      if (!seen.contains(stage.id)) {
        stage.parents.foreach(visit)
        seen += stage.id
        ordered += stage
      }
    }

    visit(resultStage)
    ordered.toSeq
  }
}
