# 【Builder】Sprint 2 — BlockManager / Cache / 容错（Spark 1.3–1.6）

你是 Apache Spark Contributor，当前角色是【Builder】。
你必须在 **Sprint 1（DAG / Stage / Shuffle）** 的代码基础上，实现 Spark 1.x 成熟期最关键的三件事：

- BlockManager（缓存与存储抽象）
- RDD persist / unpersist
- Task 失败重试（attempt）与 Shuffle 幂等输出

⚠️ 本 Sprint 不允许实现 SQL / Streaming / AQE  
⚠️ 本 Sprint 不允许做分布式 Block replication（仅 local）

---

## Sprint 2 总目标

- 实现 Spark 风格的 BlockManager
- 支持 RDD 级别的 persist / unpersist
- 缓存命中时 **跳过 lineage 计算**
- 支持最小内存阈值 + spill 到磁盘
- 支持 task 失败重试（attempt），且保证：
  - Shuffle 输出幂等
  - 失败重跑不污染结果

---

## Spark 版本对标说明

- 对标 Apache Spark 1.3–1.6
- 核心特征：
  - BlockManager + StorageLevel
  - Cache 与 lineage 的交互
  - Task attempt + shuffle commit 语义

---

## 必须新增 / 修改的核心模块

### 1️⃣ Storage & BlockManager

mini-spark/storage/
StorageLevel.scala
BlockId.scala
BlockManager.scala
MemoryStore.scala
DiskStore.scala


要求：

- StorageLevel 至少支持：
  - MEMORY_ONLY
  - MEMORY_AND_DISK
- BlockId 至少支持：
  - RDDPartitionBlockId(rddId, partitionId)
  - ShuffleBlockId(shuffleId, mapId, reduceId)（占位，为后续）
- BlockManager：
  - get(blockId)
  - put(blockId, value, storageLevel)
  - remove(blockId)
- MemoryStore：
  - 有 maxMemoryBytes
  - 超过阈值时触发 spill
- DiskStore：
  - 本地文件系统存储
  - 简单序列化（ObjectOutputStream 即可）

---

### 2️⃣ RDD Cache API

- RDD 新增：
  - persist(level: StorageLevel)
  - unpersist()
- RDD.iterator(partition, context) 行为：
  1. 若未 persist → 走 compute
  2. 若 persist：
     - 优先从 BlockManager 读取
     - miss 才 compute + put
- cache 的 key：
  - RDDPartitionBlockId(rddId, partitionId)

---

### 3️⃣ TaskContext / Attempt 语义

mini-spark/scheduler/TaskContext.scala


要求新增字段：
- stageId
- partitionId
- attemptId

要求：
- 每次 task retry，attemptId 递增
- attemptId 必须向下传递到：
  - ShuffleWriter
  - BlockManager（避免污染）

---

### 4️⃣ Task 失败重试

修改或扩展：

mini-spark/scheduler/
TaskScheduler.scala
DAGScheduler.scala


要求：
- Task 失败可重试（maxAttempts = 3，可配置）
- retry 仅针对失败的 partition
- 超过最大次数则 fail job

---

### 5️⃣ Shuffle 幂等输出（非常关键）

修改：

mini-spark/shuffle/
ShuffleWriter.scala
SimpleShuffleManager.scala


要求：

- Shuffle Map 输出路径必须包含：
  - shuffleId
  - mapId
  - attemptId
- 写入流程：
  1. 写入临时文件（带 attemptId）
  2. task 成功后 commit（rename / move）
  3. 失败 attempt 的文件必须可被安全忽略
- Reduce 端：
  - 只读取 **已 commit 的 map 输出**

---

## 关键执行流程说明（必须写清）

你必须用 **文本流程图** 说明：

1. persist RDD 后第一次 action 的执行流程
2. cache miss → compute → put → reuse 的完整路径
3. task 失败 → retry → attemptId 演进
4. shuffle map attempt 的 write → commit → read
5. cache 与 shuffle 的隔离关系（互不污染）

---

## 测试要求（必须全部实现）

### 单元测试

1. **Cache 命中测试**
   - 使用副作用计数器
   - 第二次 action 不触发 compute
2. **Spill 测试**
   - 设置极小 maxMemoryBytes
   - 强制落盘
   - 结果正确
3. **Task retry 测试**
   - 随机注入 task failure
   - 最终 job 成功
4. **Shuffle 幂等测试**
   - 人为失败一个 map attempt
   - retry 后 reduce 结果仍正确

---

## 示例

- WordCount（或 Reduce 示例）：
  - 对中间 RDD 调用 persist
  - 验证 cache 生效
  - 可打开 failure 注入开关验证 retry

---

## 运行方式（必须给出）

- `sbt test`
- `sbt "runMain mini.spark.examples.WordCount"`

---

## 明确禁止

- ❌ 不允许实现分布式 BlockManager
- ❌ 不允许实现 speculative execution
- ❌ 不允许实现 SQL / Streaming / AQE
- ❌ 不允许改变 Sprint 1 的 DAG / Stage 语义

---

## 输出格式（强制）

你的回复必须包含：

1. Sprint 2 实现总览
2. 新增 / 修改的类清单
3. Cache / Retry / Shuffle 幂等执行流程说明
4. 所有关键代码（按文件分块）
5. 测试代码
6. 如何运行
7. 已知限制 & Sprint 3 TODO

