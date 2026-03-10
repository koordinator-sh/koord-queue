# 智能队列策略端到端测试

## 概述

本测试验证智能队列策略（Intelligent Queue Strategy）的核心功能：基于优先级阈值的分层调度机制。

## 测试场景

智能队列策略采用双队列机制：
- **高优先级队列**：优先级 ≥ 阈值（默认4）的任务，按 **FIFO** 顺序调度
- **低优先级队列**：优先级 < 阈值的任务，按 **优先级+时间** 排序调度

## 测试文件

- `intelligent.queue.yaml` - 智能队列配置（阈值设为4）
- `intelligent.jobs.yaml` - 6个不同优先级的测试任务
- `test.intelligent.sh` - 自动化测试脚本

## 测试任务

测试创建6个任务，提交顺序如下：

| 提交顺序 | 任务名称 | 优先级 | 所属队列 | 预期调度顺序 |
|---------|---------|--------|---------|------------|
| 1 | high-prio-job-1 | 5 | 高优先级（FIFO） | 1 (FIFO第一) |
| 2 | high-prio-job-2 | 4 | 高优先级（FIFO） | 2 (FIFO第二) |
| 3 | high-prio-job-3 | 5 | 高优先级（FIFO） | 3 (FIFO第三) |
| 4 | low-prio-job-1  | 1 | 低优先级（优先级排序） | 6 (优先级最低) |
| 5 | low-prio-job-2  | 3 | 低优先级（优先级排序） | 4 (优先级最高) |
| 6 | low-prio-job-3  | 2 | 低优先级（优先级排序） | 5 (优先级中等) |

## 验证逻辑

### 高优先级队列验证（FIFO）

即使 `high-prio-job-3` 的优先级（5）与 `high-prio-job-1` 相同，但因为提交时间晚，应该排在后面：

- ✓ high-prio-job-1 (prio=5, 第1个提交) → position=1
- ✓ high-prio-job-2 (prio=4, 第2个提交) → position=2  
- ✓ high-prio-job-3 (prio=5, 第3个提交) → position=3

**关键验证点**：高优先级队列严格按照提交时间FIFO，不考虑优先级差异。

### 低优先级队列验证（优先级排序）

低优先级任务按照优先级高低排序，与提交时间无关：

- ✓ low-prio-job-2 (prio=3) → position=4
- ✓ low-prio-job-3 (prio=2) → position=5
- ✓ low-prio-job-1 (prio=1) → position=6

**关键验证点**：低优先级队列按优先级排序，优先级高的先调度。

## 运行测试

### 前置条件

1. Kubernetes集群已部署
2. kube-queue控制器已运行
3. 已安装 kubectl 和 jq 工具

### 执行测试

```bash
cd /Users/yueming/go/src/gitlab.alibaba-inc.com/cos/kube-queue/pkg/test/e2e
./test.intelligent.sh
```

### 预期输出

```
=== Setting up Intelligent Queue E2E Test ===
Applying ElasticQuota...
Applying Intelligent Queue...
Applying Namespace...
Applying test jobs...

=== Checking scheduling order ===
Checking queue positions...
high-prio-job-1 (priority=5): position=1
high-prio-job-2 (priority=4): position=2
high-prio-job-3 (priority=5): position=3
low-prio-job-2  (priority=3): position=4
low-prio-job-3  (priority=2): position=5
low-prio-job-1  (priority=1): position=6

=== Verification ===
✓ High priority jobs (>=4) are in FIFO order: PASS
✓ Low priority jobs (<4) are in priority order: PASS

=== TEST PASSED ✓ ===
Intelligent Queue Strategy works correctly:
  - High priority tasks (>=threshold) use FIFO ordering
  - Low priority tasks (<threshold) use priority+time ordering
```

## 清理测试环境

```bash
kubectl delete -f intelligent.jobs.yaml
kubectl delete -f intelligent.queue.yaml
kubectl delete -f namespace.yaml
kubectl delete -f elasticquotas-less-resource.yaml
```

## 调整阈值测试

如需测试不同的优先级阈值，修改 `intelligent.queue.yaml` 中的注解：

```yaml
annotations:
  kube-queue/priority-threshold: "3"  # 将阈值改为3
```

这样：
- 优先级 ≥ 3 的任务会进入高优先级队列（FIFO）
- 优先级 < 3 的任务会进入低优先级队列（优先级排序）

## 故障排查

### 查看队列状态

```bash
kubectl get queue intelligent-queue -n kube-queue -o yaml
```

### 查看任务单元状态

```bash
kubectl get queueunits -n test-group
```

### 查看任务单元详细信息

```bash
kubectl get queueunit <job-name> -n test-group -o yaml
```

### 查看控制器日志

```bash
kubectl logs -n kube-system -l app=kube-queue-controller --tail=100
```

## 测试覆盖的功能点

1. ✓ 队列策略注册和创建
2. ✓ 优先级阈值配置解析
3. ✓ 任务分配到正确的子队列（高/低优先级）
4. ✓ 高优先级队列FIFO排序
5. ✓ 低优先级队列优先级+时间排序
6. ✓ SortedList接口返回正确的排序结果
7. ✓ QueueUnit position字段正确设置

## 注意事项

- 测试脚本使用 `jq` 解析JSON输出，确保已安装
- 测试依赖于 ElasticQuota 配置，确保资源充足
- 建议在独立的测试集群运行，避免影响生产环境
- 如果测试失败，检查 kube-queue 控制器日志获取详细信息
