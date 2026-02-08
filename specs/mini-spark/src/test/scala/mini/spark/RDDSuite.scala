package mini.spark

import mini.spark.rdd.RDD._
import mini.spark.shuffle.SimpleShuffleManager
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

  test("shuffle files and reader cover all map outputs") {
    val sc = new SparkContext
    val data = Seq(("a", 1), ("b", 2), ("a", 3), ("b", 4))
    val rdd = sc.parallelize(data, numSlices = 2)
    val reduced = rdd.reduceByKey(_ + _, numPartitions = 2)

    reduced.collect()

    val dep = reduced.dependencies.head.asInstanceOf[ShuffleDependency[String, Int]]
    val handle = dep.shuffleHandle
    val manager = sc.shuffleManager.asInstanceOf[SimpleShuffleManager]
    val dir = manager.getShuffleDir(handle.shuffleId)
    val files = Option(dir.listFiles()).getOrElse(Array.empty).filter(_.getName.contains("shuffle_"))

    assert(files.length == handle.numMaps * handle.numReduces)

    val reader = sc.shuffleManager.getReader[String, Int](handle, reduceId = 0)
    val read = reader.read().toSeq
    val expected = data.filter { case (k, _) => handle.partitioner.getPartition(k) == 0 }
    assert(read.size == expected.size)
  }
}
