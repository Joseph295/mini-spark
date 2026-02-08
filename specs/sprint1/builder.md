# 【Builder】Sprint 1 — DAG / Stage / Task / Shuffle（Spark 1.0）

你是 Apache Spark Contributor，当前角色是【Builder】。
你必须在 **Sprint 0 的代码基础上** 实现 Spark 1.0 的核心执行模型。

⚠️ 你只能实现本 Sprint 要求的内容  
⚠️ 不允许提前实现 cache / persist / retry / spill（那是 Sprint 2）

---

## Sprint 1 总目标

- 在现有 RDD 内核上，引入 Spark 1.0 的执行模型：
  - Dependency（窄 / 宽）
  - DAG → Stage 切分
  - Task 抽象
  - Shuffle（Map / Reduce）
- 提供真正基于 shuffle 的 `reduceByKey`
- WordCount 必须通过 shuffle 执行（严禁 collect 后本地聚合）

---

## Spark 版本对标说明

- 对标 Apache Spark 1.0
- 核心特征：
  - DAG Scheduler
  - Stage（以 Shuffle 为边界）
  - ResultTask / ShuffleMapTask
  - 基于文件的 Shuffle Map / Reduce

---

## 必须新增 / 修改的核心模块

### 1️⃣ Dependency 体系

mini/spark/dependency/
Dependency.scala
NarrowDependency.scala
ShuffleDependency.scala


要求：
- RDD 必须暴露 `dependencies: Seq[Dependency[_]]`
- `reduceByKey` / `join` 等 **必须产生 ShuffleDependency**
- NarrowDependency 仅用于 map / filter / flatMap

---

### 2️⃣ Scheduler 体系（核心）

mini/spark/scheduler/
DAGScheduler.scala
Stage.scala
Task.scala
ResultTask.scala
ShuffleMapTask.scala
TaskScheduler.scala // 可极简（线程池或串行）


要求：
- `DAGScheduler` 负责：
  - 从 final RDD 反向遍历 lineage
  - 识别 `ShuffleDependency`
  - 构建 Stage DAG（父子关系）
  - 按拓扑顺序提交 Stage
- `Task` 至少包含：
  - stageId
  - partitionId
  - run(context): T
- `ResultTask`：
  - 用于 final stage
  - 返回 action 结果
- `ShuffleMapTask`：
  - 用于 map stage
  - 负责写 shuffle 输出

---

### 3️⃣ Shuffle 体系（最小可用）

mini/spark/shuffle/
ShuffleManager.scala
SimpleShuffleManager.scala
ShuffleWriter.scala
ShuffleReader.scala
ShuffleHandle.scala


要求：

**Map 端**
- 每个 `ShuffleMapTask`：
  - 按 reduce partition 分桶
  - 写多个本地文件
- 文件命名或路径中 **必须包含**：
  - shuffleId
  - mapId
  - reduceId

**Reduce 端**
- `ShuffleReader`：
  - 针对一个 reduceId
  - 拉取该 reduceId 对应的 **所有 map 输出**
  - 合并为 `Iterator[(K, V)]`

约束：
- 使用本地文件系统
- 不需要网络传输
- 不需要排序（可以 TODO）
- 不需要压缩

---

### 4️⃣ RDD API 扩展

- 提供 `reduceByKey`
- 可通过 `implicit class PairRDDFunctions` 实现
- `reduceByKey` 必须：
  - 创建 `ShuffleDependency`
  - 在 reduce stage 使用 `ShuffleReader`
- 严禁在 Driver 端做本地聚合

---

### 5️⃣ SparkContext 改造

- `SparkContext.runJob` 不再直接遍历 RDD
- 改为：
  - 调用 `DAGScheduler.submitJob`
  - 由 DAGScheduler：
    - 构建 stages
    - 生成 tasks
    - 调度执行

---

## 关键执行流程说明（必须写清）

你必须在输出中用 **文本流程图** 说明：

1. action（如 collect）如何触发 job
2. 如何反向遍历 RDD lineage
3. 如何识别 `ShuffleDependency`
4. 如何切分 Stage
5. Stage DAG 如何确定执行顺序
6. `ShuffleMapTask` 如何写数据
7. `ResultTask` 如何通过 `ShuffleReader` 读数据

---

## 测试要求（必须全部实现）

### 单元测试

1. `reduceByKey` 正确性测试（随机输入）
2. Stage 切分测试：
   - `map → reduceByKey → collect`
   - 至少生成 **2 个 Stage**
3. Shuffle 文件测试：
   - Map 输出文件数量正确
   - Reduce 能读取所有 map 输出

### 示例

- WordCount 示例：
  - 必须使用 `reduceByKey`
  - 严禁 `collect` 后本地聚合

---

## 运行方式（必须给出）

- `sbt test`
- `sbt "runMain mini.spark.examples.WordCount"`

---

## 明确禁止

- ❌ 不允许实现 cache / persist
- ❌ 不允许实现失败重试 / attempt
- ❌ 不允许实现 spill / 内存管理
- ❌ 不允许实现 SQL / Streaming / AQE
- ❌ 不允许绕过 ShuffleManager

---

## 输出格式（强制）

你的回复必须包含：

1. Sprint 1 实现总览
2. 新增 / 修改的类清单
3. DAG / Stage / Shuffle 执行流程说明
4. 所有关键代码（按文件分块）
5. 测试代码
6. 如何运行
7. 已知限制 & Sprint 2 TODO
