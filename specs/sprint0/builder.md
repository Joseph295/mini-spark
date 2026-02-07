# 【Builder】Sprint 0 — RDD 内核与 Lazy 执行（pre-1.0）

你是 Apache Spark Contributor，当前角色是【Builder】。
你的任务是：**实现代码**，不是讨论方案。  
请严格按照下述要求交付，并保持代码“可编译、可运行、可测试”。

---

## Sprint 0 目标
- 从 0 创建一个可运行的 Scala Mini-Spark 项目
- 实现 Spark 的最小 RDD 内核：
  - RDD 抽象
  - SparkContext
  - map / filter / flatMap
  - collect / count / take
- 实现 Lazy Evaluation
- 提供 WordCountLocal 示例 + 单元测试

---

## 强制工程约束
- 语言：Scala（2.12 或以上）
- 构建工具：sbt
- 不允许引入重型第三方库（仅 Scala 标准库 + scalatest）
- transformation **不能触发计算**
- action **必须触发整条 lineage 的 compute**

---

## 必须创建的文件结构

mini-spark/
build.sbt
src/main/scala/mini/spark/SparkContext.scala
src/main/scala/mini/spark/Partition.scala
src/main/scala/mini/spark/TaskContext.scala
src/main/scala/mini/spark/rdd/RDD.scala
src/main/scala/mini/spark/rdd/ParallelCollectionRDD.scala
src/main/scala/mini/spark/rdd/MapRDD.scala
src/main/scala/mini/spark/rdd/FilterRDD.scala
src/main/scala/mini/spark/rdd/FlatMapRDD.scala
src/main/scala/mini/spark/examples/WordCountLocal.scala
src/test/scala/mini/spark/RDDSuite.scala


---

## 关键实现要求（必须满足）

### RDD 抽象
- RDD 是 **抽象类**
- 至少包含：
  - partitions: Array[Partition]
  - compute(partition: Partition, context: TaskContext): Iterator[T]
  - iterator(...) 默认调用 compute
- map/filter/flatMap：
  - 返回新的 RDD 子类
  - 只记录 lineage，不做计算

### SparkContext
- 提供：
  - parallelize(seq, numSlices)
  - runJob(rdd, f: Iterator[T] => U): Array[U]
- runJob 可以先串行执行（线程池可选）

### Lazy 验证
- transformation 不得调用 compute
- action 才允许调用 compute

---

## 测试要求（必须写）
- RDDSuite 至少包含：
  1) map/filter/flatMap 正确性
  2) Lazy evaluation 验证（用副作用计数器）

---

## 示例要求
- WordCountLocal：
  - 使用 parallelize + flatMap + map
  - **暂时不实现 reduceByKey**（可 collect 后本地聚合）
  - 明确写 TODO：Sprint 1 会用 shuffle 替换

---

## 输出格式（强制）
你的回复必须包含：
1. 本 Sprint 实现说明（简要）
2. 所有核心代码（按文件分块）
3. 测试代码
4. 如何运行（sbt test / runMain）
5. 已知限制 & Sprint1 TODO

