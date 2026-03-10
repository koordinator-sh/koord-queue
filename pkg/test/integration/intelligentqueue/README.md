# 智能队列策略集成测试

## 概述

本目录包含智能队列策略（Intelligent Queue Strategy）的集成测试套件，使用Ginkgo测试框架验证队列的核心功能。

## 测试场景

集成测试覆盖以下场景：

### 1. 高优先级队列优先级+时间排序测试

**测试用例**: `High Priority Queue Priority+FIFO Ordering`

**验证内容**:
- 优先级 ≥ 阈值（4）的任务先按优先级排序，优先级相同时按FIFO顺序
- 高优先级的任务排在前面
- 同等优先级的任务按提交时间先后排序

**测试数据**:
```
high-prio-job-3: priority=6, 第3个提交 → 预期位置 1 (最高优先级)
high-prio-job-1: priority=5, 第1个提交 → 预期位置 2 (第二优先级)
high-prio-job-2: priority=4, 第2个提交 → 预期位置 3 (最低优先级)
```

**关键验证点**:
- `high-prio-job-3`虽然最后提交，但因为优先级最高（6），排在第1位
- `high-prio-job-2`虽然优先级最低（4），排在最后
- 这验证了高优先级队列先按优先级排序，优先级相同时再按FIFO排序

### 2. 低优先级队列优先级排序测试

**测试用例**: `Low Priority Queue Priority Ordering`

**验证内容**:
- 优先级 < 阈值（4）的任务按优先级高低排序
- 与提交时间无关，仅看优先级
- 优先级高的排在前面

**测试数据**:
```
low-prio-job-1: priority=1, 第1个提交 → 预期位置 3 (优先级最低)
low-prio-job-2: priority=3, 第2个提交 → 预期位置 1 (优先级最高)
low-prio-job-3: priority=2, 第3个提交 → 预期位置 2 (优先级中等)
```

**关键验证点**:
- `low-prio-job-2`虽然最后提交，但因为优先级最高（3），排在第1位
- `low-prio-job-1`虽然最先提交，但因为优先级最低（1），排在最后
- 这验证了低优先级队列仅按优先级排序，不考虑提交时间

### 3. 混合优先级队列排序测试

**测试用例**: `Mixed Priority Queue Ordering`

**验证内容**:
- 高优先级任务总是排在低优先级任务前面
- 高优先级队列内部优先级+时间排序
- 低优先级队列内部优先级排序
- 两个队列正确协同工作

**测试数据**:
```
high-job-1: priority=5 → 预期位置 1 (高优先级队列，优先级最高)
high-job-2: priority=4 → 预期位置 2 (高优先级队列，优先级最低)
low-job-2:  priority=3 → 预期位置 3 (低优先级队列，优先级最高)
low-job-1:  priority=1 → 预期位置 4 (低优先级队列，优先级最低)
```

**关键验证点**:
- 所有高优先级任务（≥4）都排在低优先级任务（<4）前面
- 验证了双队列机制的正确性

### 4. 优先级阈值边界测试

**测试用例**: `Priority Threshold Boundary`

**验证内容**:
- 优先级恰好等于阈值（4）的任务归类正确
- 阈值边界任务应归入高优先级队列
- 低于阈值的任务归入低优先级队列
- 高于阈值的任务归入高优先级队列

**测试数据**:
```
above-threshold-job: priority=5 → 预期位置 1 (高于阈值，高优先级队列，优先级最高)
threshold-job:       priority=4 → 预期位置 2 (等于阈值，高优先级队列)
below-threshold-job: priority=3 → 预期位置 3 (低于阈值，低优先级队列)
```

**关键验证点**:
- `threshold-job`（priority=4）应该在高优先级队列，不是低优先级队列
- 高优先级队列内部按优先级排序（above-threshold-job 在 threshold-job 前面）
- 验证了边界条件处理的正确性（priority ≥ threshold → 高优先级）

## 测试框架

### 核心验证机制

集成测试通过检查 Queue 对象的 Status 来验证任务排序，而不是检查单个 QueueUnit:

```go
// 获取队列对象
queue, err := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(
    context.TODO(), "test-intelligent-queue", metav1.GetOptions{})

// 从 QueueItemDetails 中查找任务位置
for _, item := range queue.Status.QueueItemDetails["active"] {
    if item.Name == "high-prio-job-1" {
        return item.Position  // QueueItemDetail 包含 Position 字段
    }
}
```

**关键数据结构**:
- `Queue.Status.QueueItemDetails`: 包含队列中所有任务的详细信息
- `QueueItemDetail.Position`: 任务在队列中的位置（1-based）
- `QueueItemDetail` 由队列的 `SortedList()` 方法生成并定期更新到 Queue 的 Status 中

**注意**: `QueueUnit.Status` 不包含 `Position` 字段，位置信息只在 Queue 对象中维护。

**重要**：为了验证队列位置，测试环境配置了零资源配额（max=0），这样任务会保持在队列中而不会被立即调度。如果有足够资源，任务会被立即出队（Dequeued），此时 `QueueItemDetails` 将为空，因为 `SortedList()` 只包含当前在队列中等待的任务。

### 技术栈
- **测试框架**: Ginkgo v2 + Gomega
- **测试类型**: 集成测试
- **运行环境**: 内存中的fake Kubernetes API

### 测试结构

```
BeforeAll()
  ├── 初始化测试环境
  ├── 创建Framework和Controller
  ├── 创建ElasticQuotaTree
  ├── 创建智能队列（阈值=4）
  └── 启动控制器

Describe("测试场景")
  ├── BeforeAll() - 准备测试数据
  ├── It("测试用例") - 执行测试
  └── AfterEach() - 清理资源

AfterAll()
  └── 停止控制器
```

## 运行测试

### 前置条件

1. Go 1.24+
2. Ginkgo测试框架已安装

### 运行全部测试

```bash
cd /Users/yueming/go/src/gitlab.alibaba-inc.com/cos/kube-queue

# 运行智能队列集成测试
go test -v ./pkg/test/integration/intelligentqueue/
```

### 运行特定测试

```bash
# 使用Ginkgo的focus功能运行特定测试
go test -v ./pkg/test/integration/intelligentqueue/ -ginkgo.focus="FIFO"
```

### 查看详细输出

```bash
# 启用详细日志
go test -v ./pkg/test/integration/intelligentqueue/ -ginkgo.v
```

## 预期输出

成功运行时的输出示例：

```
Running Suite: IntelligentQueue Suite - /Users/yueming/go/src/.../intelligentqueue
================================================================
Random Seed: 1234567890

Will run 4 of 4 specs
••••

Ran 4 of 4 Specs in 15.234 seconds
SUCCESS! -- 4 Passed | 0 Failed | 0 Pending | 0 Skipped
PASS
```

## 测试覆盖的代码路径

### 核心功能
- ✅ `IntelligentQueue.NewIntelligentQueue()` - 队列创建
- ✅ `IntelligentQueue.AddQueueUnitInfo()` - 任务添加和分类
- ✅ `IntelligentQueue.priorityLess()` - 优先级+时间比较函数（用于高低优先级队列）
- ✅ `IntelligentQueue.findNextQueueUnit()` - 双队列出队
- ✅ `IntelligentQueue.SortedList()` - 排序列表生成

### 边界情况
- ✅ 优先级等于阈值的任务分类
- ✅ 高低优先级任务混合场景
- ✅ 相同优先级任务的排序
- ✅ 双队列协同工作

### 集成点
- ✅ 与Framework的集成
- ✅ 与ElasticQuota的集成
- ✅ 与Scheduler的集成
- ✅ 与Controller的集成

## 故障排查

### 测试失败

如果测试失败，检查以下内容：

1. **位置不匹配**
   ```
   Expected position: 1
   Actual position: 2
   ```
   可能原因：
   - 队列排序逻辑错误
   - 优先级阈值配置错误
   - 任务分类到错误的子队列
   - Queue Status 未及时更新（增加等待时间）

2. **超时错误**
   ```
   Timed out after 10s waiting for position
   ```
   可能原因：
   - 队列未正确创建
   - Controller未启动
   - 任务未被处理

3. **资源创建失败**
   ```
   Error creating queue unit
   ```
   可能原因：
   - ElasticQuota未创建
   - Namespace配置错误
   - Queue不存在

### 调试技巧

1. **启用详细日志**
   ```bash
   go test -v ./pkg/test/integration/intelligentqueue/ -ginkgo.v -v=4
   ```

2. **检查队列状态**
   在测试中添加调试输出：
   ```go
   q, _ := fw.QueueUnitClient().SchedulingV1alpha1().Queues("kube-queue").Get(...)
   fmt.Printf("Queue: %+v\n", q)
   ```

3. **检查任务状态**
   ```go
   qu, _ := fw.QueueUnitClient().SchedulingV1alpha1().QueueUnits("default").Get(...)
   fmt.Printf("QueueUnit: %+v\n", qu.Status)
   ```

## 扩展测试

可以基于现有测试框架添加以下测试场景：

### 动态阈值调整
```go
It("should reorder jobs when threshold changes", func() {
    // 1. 创建混合优先级任务
    // 2. 验证初始排序
    // 3. 修改队列阈值
    // 4. 验证任务重新排序
})
```

### 任务更新场景
```go
It("should reorder when job priority changes", func() {
    // 1. 创建任务
    // 2. 更新任务优先级（跨越阈值）
    // 3. 验证任务在队列间迁移
    // 4. 验证新的排序
})
```

### 大规模任务测试
```go
It("should handle 100+ jobs correctly", func() {
    // 1. 创建100个不同优先级的任务
    // 2. 验证排序正确性
    // 3. 验证性能可接受
})
```

### 并发提交测试
```go
It("should handle concurrent job submissions", func() {
    // 1. 并发创建多个任务
    // 2. 验证线程安全
    // 3. 验证排序正确性
})
```

## 与单元测试的对比

| 维度 | 单元测试 | 集成测试 |
|------|---------|---------|
| 测试范围 | 单个函数/方法 | 完整的调度流程 |
| 依赖 | Mock对象 | 真实的Framework和Controller |
| 运行速度 | 快（毫秒级） | 慢（秒级） |
| 隔离性 | 高 | 低 |
| 真实性 | 低 | 高 |
| 覆盖度 | 代码逻辑 | 端到端流程 |

**建议**：
- 单元测试用于验证各个函数的正确性
- 集成测试用于验证整体流程和组件协作
- 两者结合，确保全面的测试覆盖

## 参考文档

- [Ginkgo测试框架文档](https://onsi.github.io/ginkgo/)
- [智能队列策略设计文档](../../../.qoder/quests/intelligent-queue-strategy-design.md)
- [BaseQueue通用方法说明](../../queue/queuepolicies/basequeue/README.md)
