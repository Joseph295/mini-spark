package mini.spark

import mini.spark.rdd.RDD._
import mini.spark.shuffle.SimpleShuffleManager
import mini.spark.storage.{RDDPartitionBlockId, StorageLevel}
import org.scalatest.funsuite.AnyFunSuite

class RDDSuite extends AnyFunSuite {
  test("map/filter/flatMap correctness") {
    val sc = new SparkContext
    val rdd = sc.parallelize(Seq(1, 2, 3, 4), numSlices = 2)

    val result = rdd
      .map(_ * 2)
      .filter(_ > 4)
      .flatMap(x => Seq(x, x + 1))
      .collect()

    assert(result.sameElements(Array(6, 7, 8, 9)))
  }

  test("lazy evaluation triggers only on action") {
    val sc = new SparkContext
    var counter = 0

    val rdd = sc.parallelize(Seq(1, 2, 3), numSlices = 1).map { x =>
      counter += 1
      x + 1
    }

    assert(counter == 0)

    val collected = rdd.collect()
    assert(collected.sameElements(Array(2, 3, 4)))
    assert(counter == 3)

    val total = rdd.count()
    assert(total == 3)
    assert(counter == 6)
  }

  test("take triggers shuffle job for reduceByKey RDDs") {
    val sc = new SparkContext
    val data = Seq(("a", 1), ("b", 2), ("a", 3))
    val rdd = sc.parallelize(data, numSlices = 2).reduceByKey(_ + _)

    val result = rdd.take(1)
    assert(result.nonEmpty)
  }

  test("cache hit avoids recompute") {
    val sc = new SparkContext
    var counter = 0

    val rdd = sc
      .parallelize(Seq(1, 2, 3), numSlices = 1)
      .map { x =>
        counter += 1
        x
      }
      .persist(StorageLevel.MEMORY_ONLY)

    val first = rdd.collect()
    val second = rdd.collect()

    assert(first.sameElements(second))
    assert(counter == 3)
  }

  test("spill to disk when memory is insufficient") {
    val sc = new SparkContext(maxMemoryBytes = 1)
    var counter = 0

    val rdd = sc
      .parallelize(Seq(1, 2, 3), numSlices = 1)
      .map { x =>
        counter += 1
        x
      }
      .persist(StorageLevel.MEMORY_AND_DISK)

    val first = rdd.collect()
    val second = rdd.collect()

    assert(first.sameElements(second))
    assert(counter == 3)

    val blockId = RDDPartitionBlockId(rdd.id, 0)
    assert(sc.blockManager.memoryStore.get(blockId).isEmpty)
    assert(sc.blockManager.diskStore.get(blockId).nonEmpty)
  }

  test("task retry succeeds after failure") {
    val sc = new SparkContext
    val base = sc.parallelize(Seq(1, 2, 3), numSlices = 1)

    class FailOnceRDD(prev: mini.spark.rdd.RDD[Int]) extends mini.spark.rdd.RDD[Int](prev.sc) {
      override def partitions: Array[Partition] = prev.partitions
      override def dependencies: Seq[Dependency[_]] = Seq(new NarrowDependency(prev))

      override protected def compute(partition: Partition, context: TaskContext): Iterator[Int] = {
        if (partition.index == 0 && context.attemptId == 0) {
          throw new RuntimeException("Injected failure")
        }
        prev.iterator(partition, context)
      }
    }

    val rdd = new FailOnceRDD(base)
    val result = rdd.collect()
    assert(result.sameElements(Array(1, 2, 3)))
  }

  test("cache retry does not commit failed attempt") {
    val sc = new SparkContext(maxTaskAttempts = 2)
    var counter = 0

    val base = sc
      .parallelize(Seq(1, 2, 3), numSlices = 1)
      .map { x =>
        counter += 1
        x
      }
      .persist(StorageLevel.MEMORY_ONLY)

    class FailOnceRDD(prev: mini.spark.rdd.RDD[Int]) extends mini.spark.rdd.RDD[Int](prev.sc) {
      override def partitions: Array[Partition] = prev.partitions
      override def dependencies: Seq[Dependency[_]] = Seq(new NarrowDependency(prev))

      override protected def compute(partition: Partition, context: TaskContext): Iterator[Int] = {
        val data = prev.iterator(partition, context).toVector
        if (context.attemptId == 0) {
          throw new RuntimeException("Injected cache failure")
        }
        data.iterator
      }
    }

    val failing = new FailOnceRDD(base)
    val result = failing.collect()
    assert(result.sameElements(Array(1, 2, 3)))
    assert(counter == 6)

    val cached = base.collect()
    assert(cached.sameElements(Array(1, 2, 3)))
    assert(counter == 6)
  }

  test("shuffle idempotent output with retry") {
    val sc = new SparkContext
    val data = Seq(("a", 1), ("b", 2), ("a", 3), ("b", 4))
    val base = sc.parallelize(data, numSlices = 2)

    class FailOnceRDD(prev: mini.spark.rdd.RDD[(String, Int)])
        extends mini.spark.rdd.RDD[(String, Int)](prev.sc) {
      override def partitions: Array[Partition] = prev.partitions
      override def dependencies: Seq[Dependency[_]] = Seq(new NarrowDependency(prev))

      override protected def compute(partition: Partition, context: TaskContext): Iterator[(String, Int)] = {
        if (partition.index == 0 && context.attemptId == 0) {
          throw new RuntimeException("Injected shuffle failure")
        }
        prev.iterator(partition, context)
      }
    }

    val failing = new FailOnceRDD(base)
    val reduced = failing.reduceByKey(_ + _)
    val result = reduced.collect().toSeq.sortBy(_._1)
    val expected = data
      .groupBy(_._1)
      .map { case (k, items) => (k, items.map(_._2).sum) }
      .toSeq
      .sortBy(_._1)

    assert(result == expected)
  }

  test("explicit cleanup removes shuffle and disk cache") {
    val sc = new SparkContext(maxMemoryBytes = 1)
    val data = Seq(("a", 1), ("b", 2), ("a", 3))
    val base = sc
      .parallelize(data, numSlices = 1)
      .map(identity)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val reduced = base.reduceByKey(_ + _)
    reduced.collect()

    val dep = reduced.dependencies.head.asInstanceOf[ShuffleDependency[String, Int]]
    val handle = dep.shuffleHandle
    val manager = sc.shuffleManager.asInstanceOf[SimpleShuffleManager]
    val shuffleDir = manager.getShuffleDir(handle.shuffleId)

    val blockId = RDDPartitionBlockId(base.id, 0)
    assert(sc.blockManager.diskStore.get(blockId).nonEmpty)
    assert(!shuffleDir.exists() || Option(shuffleDir.listFiles()).getOrElse(Array.empty).isEmpty)

    sc.stop()

    assert(sc.blockManager.diskStore.get(blockId).isEmpty)
  }

  test("parallelize respects numSlices, including non-positive") {
    val sc = new SparkContext
    val rdd2 = sc.parallelize(Seq(1, 2, 3, 4), numSlices = 2)
    assert(rdd2.partitions.length == 2)

    val rdd1 = sc.parallelize(Seq(1, 2, 3), numSlices = 0)
    assert(rdd1.partitions.length == 1)
  }

  test("reduceByKey correctness with random input") {
    val sc = new SparkContext
    val rand = new scala.util.Random(42)
    val data = Seq.fill(100) {
      val k = s"k${rand.nextInt(5)}"
      val v = rand.nextInt(10)
      (k, v)
    }

    val rdd = sc.parallelize(data, numSlices = 3)
    val result = rdd.reduceByKey(_ + _).collect().toSeq.sortBy(_._1)
    val expected = data
      .groupBy(_._1)
      .map { case (k, items) => (k, items.map(_._2).sum) }
      .toSeq
      .sortBy(_._1)

    assert(result == expected)
  }

  test("stage splitting produces map stage + result stage") {
    val sc = new SparkContext
    val rdd = sc.parallelize(Seq(("a", 1), ("b", 2), ("a", 3)), numSlices = 2)
    rdd.map { case (k, v) => (k, v) }.reduceByKey(_ + _).collect()

    val stages = sc.dagScheduler.getLastJobStages
    assert(stages.length == 2)
    assert(stages.count(_.isShuffleMap) == 1)
  }

  test("shuffle cleanup happens after job") {
    val sc = new SparkContext
    val data = Seq(("a", 1), ("b", 2), ("a", 3), ("b", 4))
    val rdd = sc.parallelize(data, numSlices = 2)
    val reduced = rdd.reduceByKey(_ + _, numPartitions = 2)

    val result = reduced.collect().toSeq.sortBy(_._1)
    val expected = data
      .groupBy(_._1)
      .map { case (k, items) => (k, items.map(_._2).sum) }
      .toSeq
      .sortBy(_._1)
    assert(result == expected)

    val dep = reduced.dependencies.head.asInstanceOf[ShuffleDependency[String, Int]]
    val handle = dep.shuffleHandle
    val manager = sc.shuffleManager.asInstanceOf[SimpleShuffleManager]
    val dir = manager.getShuffleDir(handle.shuffleId)
    val files = Option(dir.listFiles()).getOrElse(Array.empty).filter(_.getName.contains("shuffle_"))
    assert(files.isEmpty)
  }
}
