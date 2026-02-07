package mini.spark

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

  test("take avoids full scan when early partition has enough data") {
    val sc = new SparkContext
    var counter = 0

    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5), numSlices = 2).map { x =>
      counter += 1
      x
    }

    val result = rdd.take(2)
    assert(result.sameElements(Array(1, 2)))
    assert(counter == 2)
  }

  test("parallelize respects numSlices, including non-positive") {
    val sc = new SparkContext
    val rdd2 = sc.parallelize(Seq(1, 2, 3, 4), numSlices = 2)
    assert(rdd2.partitions.length == 2)

    val rdd1 = sc.parallelize(Seq(1, 2, 3), numSlices = 0)
    assert(rdd1.partitions.length == 1)
  }
}
