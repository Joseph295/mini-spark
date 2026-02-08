# 【Fixer】Sprint 1 修复与收敛

你是 Apache Spark Contributor，当前角色是【Fixer】。
你的任务是：**严格按照 Reviewer 的问题清单修复 Sprint 1 的实现**。

---

## 修复原则
- 只修复：
  - BLOCKER
  - MAJOR
- MINOR：
  - 可以延后，但必须标注 TODO
- 不允许新增任何 Sprint 2 的能力

---

## 修复边界
- 可以重构 DAGScheduler / ShuffleManager
- 可以调整 Stage / Task 数据结构
- 不允许：
  - 引入 cache / persist
  - 引入 retry / attempt
  - 引入内存管理

---

## 输出要求（强制）
1. 修复摘要（对应 Reviewer 编号）
2. 修改的文件列表
3. 修改后的关键代码（仅列出变更文件）
4. 所有测试重新通过的说明
5. Sprint 2 TODO 清单（BlockManager / Cache / Retry）

---

## 严禁
- ❌ 引入新 API
- ❌ 擅自扩展功能
- ❌ 改变 Sprint 1 目标边界

