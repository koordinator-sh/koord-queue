# BaseQueue 通用队列基类

## 概述

`BaseQueue` 是所有队列策略实现的抽象基类，提供了队列管理的通用功能和数据结构。它封装了队列调度中的常见逻辑，包括任务管理、状态跟踪、配额控制、并发安全等核心功能。

## 设计目的

1. **代码复用**：避免在不同队列策略中重复实现相同的逻辑
2. **统一接口**：为所有队列策略提供一致的基础设施
3. **简化开发**：新的队列策略只需关注特定的调度逻辑，而不必处理底层细节
4. **维护性**：集中管理通用功能，便于统一修复和优化

## 核心数据结构

### 并发控制字段

```go
Lock        sync.RWMutex   // 读写锁，保护队列数据的并发访问
Cond        *sync.Cond     // 条件变量，用于协调调度器的阻塞和唤醒
WakeUpTimer *time.Timer    // 定时唤醒器，用于定时解除队列阻塞
```

### 队列基本信息

```go
Name             string        // 队列名称
ResetNextIdxFlag bool          // 下次索引重置标志，提示调度器重新开始遍历
LastResetTime    time.Time     // 上次重置时间
MaxDepth         int32         // 最大深度限制（-1表示无限制）
SessionId        int64         // 会话ID，用于跟踪队列状态变更
```

### 任务管理

```go
QueueUnits map[string]*framework.QueueUnitInfo  // 所有任务单元，key为namespace/name
Assumed    map[string]struct{}                  // 已假定（预留资源但未运行）的任务
Updating   map[string]struct{}                  // 正在更新中的任务
```

**说明**：
- `QueueUnits`: 存储队列中所有的任务单元信息
- `Assumed`: 追踪已经分配资源但Pod尚未全部Running的任务，用于资源预留
- `Updating`: 标记正在更新的任务，避免并发更新冲突

### 配额和阻塞管理

```go
QueueCr      *v1alpha1.Queue  // 队列的CR（Custom Resource）对象
Blocked      bool             // 队列是否为阻塞模式（Block策略）
BlockedQuota SyncInt          // 被阻塞的配额列表（线程安全的map）
```

**阻塞模式说明**：
- 在Block策略下，如果某个配额已满，使用该配额的任务会被标记为blocked
- 避免重复尝试调度无法满足的任务，提升调度效率

### 调度跟踪

```go
LastScheduledTime      time.Time  // 上次成功调度的时间
LastScheduledQueueUnit string     // 上次成功调度的任务名称（namespace/name）
```

**用途**：
- 防止同一任务被频繁重试（5秒内重试同一任务会触发节流）
- 检测队列死锁（3分钟内无调度活动会清空blocked配额）

### 配置选项

```go
WaitPodsRunning bool           // 是否等待Pod运行后才释放配额
EnablePreempt   bool           // 是否启用抢占功能
PreemptFlag     atomic.Int32   // 抢占标志（原子操作）
```

### 客户端和Lister

```go
Client          versioned.Interface                   // Kubernetes客户端，用于操作QueueUnit资源
QueueUnitLister externalv1alpha1.QueueUnitLister     // QueueUnit列表器，用于高效查询
Fw              framework.Handle                      // 框架句柄，提供调度器核心功能
```

## 通用方法说明

### 1. 构造和初始化

#### `NewBaseQueue`
```go
func NewBaseQueue(name string, q *v1alpha1.Queue, fw framework.Handle, 
                  client versioned.Interface, queueUnitLister externalv1alpha1.QueueUnitLister) *BaseQueue
```

**功能**：创建并初始化一个BaseQueue实例

**参数**：
- `name`: 队列名称
- `q`: 队列的CR对象
- `fw`: 框架句柄
- `client`: Kubernetes客户端
- `queueUnitLister`: QueueUnit列表器

**初始化内容**：
- 创建所有必要的数据结构（QueueUnits、Assumed、Updating等）
- 初始化条件变量Cond
- 根据Queue的Annotations设置WaitPodsRunning和EnablePreempt
- 设置Blocked标志（根据QueuePolicy是否为"Block"）
- 初始化MaxDepth为-1（无限制）

---

### 2. 任务调度相关方法

#### `ShouldSkipQueueUnit`
```go
func (bq *BaseQueue) ShouldSkipQueueUnit(qu *framework.QueueUnitInfo) bool
```

**功能**：判断一个任务是否应该在调度时跳过

**跳过条件**：
1. 任务已经满足（Satisfied状态）
2. 任务使用的配额被阻塞（在BlockedQuota中）

**使用场景**：在队列遍历时过滤不应调度的任务

---

#### `GetQueueMaxResetDuration`
```go
func (bq *BaseQueue) GetQueueMaxResetDuration(queueLen int) time.Duration
```

**功能**：计算队列索引重置的最大时间间隔

**返回值**：
- 队列长度 > 500（或MaxDepth > 500）：5分钟
- 其他情况：3分钟

**用途**：防止队列遍历长期停留在某个位置，定期重置以保证公平性

---

#### `UpdateScheduledInfo`
```go
func (bq *BaseQueue) UpdateScheduledInfo(qu *framework.QueueUnitInfo)
```

**功能**：更新最后调度成功的任务信息

**更新内容**：
- LastScheduledQueueUnit：任务的namespace/name
- LastScheduledTime：当前时间

**用途**：用于节流检测和死锁检测

---

### 3. 阻塞和流控管理

#### `HandleBlockedQueue`
```go
func (bq *BaseQueue) HandleBlockedQueue(ctx context.Context, qu *framework.QueueUnitInfo, 
                                         hasMoreUnits bool) (shouldBlock bool, shouldRejudge bool)
```

**功能**：处理阻塞模式队列的调度逻辑

**参数**：
- `qu`: 当前要调度的任务（nil表示无任务可调度）
- `hasMoreUnits`: 是否还有更多任务待处理

**返回值**：
- `shouldBlock`: 是否应该阻塞等待
- `shouldRejudge`: 是否应该重新评估（清空blocked配额）

**处理逻辑**：

1. **当qu为nil（无可调度任务）**：
   - 检查距离上次调度是否超过3分钟
   - 如果超时且有blocked配额：清空BlockedQuota，返回shouldRejudge=true
   - 否则：设置30秒定时器后唤醒，返回shouldBlock=true

2. **当qu不为nil但触发节流**：
   - 如果距离上次调度 < 5秒，且是同一任务
   - 设置5秒定时器后唤醒，返回shouldBlock=true

**用途**：避免无效调度尝试，同时防止队列永久死锁

---

#### `HandleNonBlockedQueue`
```go
func (bq *BaseQueue) HandleNonBlockedQueue(ctx context.Context, qu *framework.QueueUnitInfo)
```

**功能**：处理非阻塞模式队列的调度逻辑

**处理逻辑**：
- 如果qu为nil且有blocked配额：清空BlockedQuota并设置重置标志
- 如果qu为nil且无blocked配额：设置1分钟定时器并等待

**用途**：为非阻塞模式提供简单的等待机制

---

### 4. 状态管理方法

#### `UpdateAssumedState`
```go
func (bq *BaseQueue) UpdateAssumedState(key string, qu *v1alpha1.QueueUnit, updating bool)
```

**功能**：更新任务的assumed（假定）状态

**管理规则**：
1. 如果任务未预留任何资源：从Assumed中删除
2. 如果任务所有Pod都已Running且不在updating状态：从Assumed中删除并设置重置标志
3. 如果任务有Pod未Running：添加到Assumed

**用途**：跟踪资源预留状态，协调调度和实际运行的差异

---

#### `CleanupDeletedUnit`
```go
func (bq *BaseQueue) CleanupDeletedUnit(qu *v1alpha1.QueueUnit)
```

**功能**：清理已删除任务的所有状态

**清理内容**：
- 从QueueUnits中删除
- 从Assumed中删除
- 从Updating中删除
- 如果是阻塞模式：从BlockedQuota中删除其使用的配额
- 如果任务曾被assumed或预留资源：增加SessionId并设置重置标志

**用途**：保持队列数据一致性，及时释放资源

---

#### `AddUnschedulableUnit`
```go
func (bq *BaseQueue) AddUnschedulableUnit(ctx context.Context, qi *framework.QueueUnitInfo, 
                                           allocatableChangedDuringScheduling bool)
```

**功能**：处理不可调度的任务

**处理逻辑**：
- 在阻塞模式且资源未变化时：将任务使用的配额添加到BlockedQuota
- 从Assumed和Updating中删除该任务

**用途**：标记阻塞的配额，避免重复尝试

---

### 5. 资源释放和状态转换

#### `HandleResourceRelease`
```go
func (bq *BaseQueue) HandleResourceRelease(old, new *v1alpha1.QueueUnit)
```

**功能**：处理任务资源释放事件

**触发条件**：
- 任务从预留状态变为释放状态
- 任务失败或完成

**处理逻辑**：
- 设置ResetNextIdxFlag（除非是SchedFailed）
- 在阻塞模式下：从BlockedQuota中删除相应配额
- 广播Cond唤醒等待的调度器

**用途**：及时响应资源释放，触发新的调度尝试

---

#### `HandleDequeueTransition`
```go
func (bq *BaseQueue) HandleDequeueTransition(old, new *v1alpha1.QueueUnit) bool
```

**功能**：处理任务从Reserved到Dequeued的状态转换

**返回值**：bool，表示是否发生了转换

**处理逻辑**：
- 如果任务变为Dequeued且在Assumed中：
  - 从Assumed中删除
  - 设置重置标志
  - 如果WaitPodsRunning=true：清空所有BlockedQuota
  - 广播唤醒调度器

**用途**：当任务的Pod开始运行时，释放阻塞的配额

---

### 6. 辅助工具方法

#### `PodSetsChanged`
```go
func PodSetsChanged(old, new *v1alpha1.QueueUnit) bool
```

**功能**：检查任务的PodSets规格是否发生变化

**比较内容**：
- PodSet数量是否变化
- 每个PodSet的Count是否变化

**用途**：检测任务规格变更，决定是否需要重新调度

---

#### `IsAdmissionChanged`
```go
func IsAdmissionChanged(old, new *v1alpha1.QueueUnit) bool
```

**功能**：检查任务的Admission状态是否发生变化

**比较内容**：
- Admission数量
- 每个Admission的Name、Running、Replicas字段

**用途**：检测任务资源分配变化，触发状态同步

---

#### `List`
```go
func (bq *BaseQueue) List() []*framework.QueueUnitInfo
```

**功能**：返回队列中所有任务的列表

**返回值**：QueueUnitInfo切片

**用途**：查询队列状态、调试、监控等

---

### 7. 默认实现方法（可被子类覆盖）

#### `PendingLength`
```go
func (bq *BaseQueue) PendingLength() int
```

**功能**：返回待处理任务数量

**默认实现**：返回QueueUnits的长度

**建议**：子类应根据实际的pending定义覆盖此方法

---

#### `Length`
```go
func (bq *BaseQueue) Length() int
```

**功能**：返回队列中任务总数

**默认实现**：返回QueueUnits的长度

---

#### `Run`
```go
func (bq *BaseQueue) Run()
```

**功能**：启动队列

**默认实现**：空操作

**建议**：如需后台任务（如定时器、监控等），在子类中覆盖

---

#### `Close`
```go
func (bq *BaseQueue) Close()
```

**功能**：关闭队列

**默认实现**：空操作

**建议**：如有需要清理的资源，在子类中覆盖

---

#### `UpdateQueueLimitByParent`
```go
func (bq *BaseQueue) UpdateQueueLimitByParent(limit bool, limitParentQuotas []string)
```

**功能**：根据父队列更新配额限制

**默认实现**：空操作

**建议**：在需要层级配额管理的策略中实现

---

## SyncInt 辅助类型

BaseQueue使用了一个线程安全的整数map类型：

```go
type SyncInt struct {
    sync.Map
}

func (s *SyncInt) Store(key string, value int)
func (s *SyncInt) Load(key string) (int, bool)
func (s *SyncInt) Delete(key string)
```

**用途**：存储BlockedQuota等需要并发访问的整数映射

---

## 常量定义

```go
const (
    WaitForPodsRunningAnnotation  = "kube-queue/wait-for-pods-running"
    EnableQueueUnitPreemption     = "kube-queue/enable-queueunit-preemption"
    MaxDepthAnnotation            = "kube-queue/max-depth"
)
```

这些常量用于从Queue的Annotations中读取配置。

---

## 使用示例

### 创建新的队列策略

```go
package myqueue

import (
    "github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies/basequeue"
    "github.com/koordinator-sh/koord-queue/pkg/framework"
    // ... 其他导入
)

// MyQueue 自定义队列策略
type MyQueue struct {
    *basequeue.BaseQueue
    
    // 添加自己特有的字段
    myQueue []*framework.QueueUnitInfo
}

// NewMyQueue 创建队列实例
func NewMyQueue(name string, q *v1alpha1.Queue, fw framework.Handle, 
                client versioned.Interface, queueUnitLister externalv1alpha1.QueueUnitLister) queuepolicies.SchedulingQueue {
    
    // 创建BaseQueue
    base := basequeue.NewBaseQueue(name, q, fw, client, queueUnitLister)
    
    return &MyQueue{
        BaseQueue: base,
        myQueue:   make([]*framework.QueueUnitInfo, 0),
    }
}

// Pop 实现弹出逻辑
func (q *MyQueue) Pop(ctx context.Context) (*framework.QueueUnitInfo, error) {
    q.Lock.Lock()
    defer q.Lock.Unlock()
    
    // 使用BaseQueue的通用方法
    for _, qu := range q.myQueue {
        if q.BaseQueue.ShouldSkipQueueUnit(qu) {
            continue
        }
        
        // 更新调度信息
        q.BaseQueue.UpdateScheduledInfo(qu)
        
        return qu, nil
    }
    
    // 处理阻塞逻辑
    if q.Blocked {
        shouldBlock, shouldRejudge := q.BaseQueue.HandleBlockedQueue(ctx, nil, false)
        if shouldRejudge {
            // 重新评估
            return q.Pop(ctx)
        }
        if shouldBlock {
            q.Cond.Wait()
            return q.Pop(ctx)
        }
    }
    
    return nil, nil
}

// AddQueueUnitInfo 添加任务到队列
func (q *MyQueue) AddQueueUnitInfo(ctx context.Context, qi *framework.QueueUnitInfo) bool {
    q.Lock.Lock()
    defer q.Lock.Unlock()
    
    key := qi.Unit.Namespace + "/" + qi.Unit.Name
    
    // 使用BaseQueue的QueueUnits
    q.QueueUnits[key] = qi
    
    // 添加到自己的队列并排序
    q.myQueue = append(q.myQueue, qi)
    // ... 排序逻辑
    
    q.Cond.Broadcast()
    return true
}

// UpdateQueueUnitInfo 更新任务
func (q *MyQueue) UpdateQueueUnitInfo(ctx context.Context, old, new *v1alpha1.QueueUnit) {
    q.Lock.Lock()
    defer q.Lock.Unlock()
    
    key := new.Namespace + "/" + new.Name
    
    // 使用BaseQueue的资源释放处理
    q.BaseQueue.HandleResourceRelease(old, new)
    
    // 使用BaseQueue的状态转换处理
    if q.BaseQueue.HandleDequeueTransition(old, new) {
        return
    }
    
    // 使用BaseQueue的assumed状态管理
    _, updating := q.Updating[key]
    q.BaseQueue.UpdateAssumedState(key, new, updating)
    
    // ... 其他更新逻辑
}

// DeleteQueueUnitInfo 删除任务
func (q *MyQueue) DeleteQueueUnitInfo(ctx context.Context, qu *v1alpha1.QueueUnit) {
    q.Lock.Lock()
    defer q.Lock.Unlock()
    
    // 使用BaseQueue的清理方法
    q.BaseQueue.CleanupDeletedUnit(qu)
    
    // 清理自己的队列
    // ... 从myQueue中删除
}
```

---

## 并发安全说明

### 锁的使用

BaseQueue提供了`Lock`（RWMutex）来保护共享数据：

```go
// 写操作
q.Lock.Lock()
defer q.Lock.Unlock()
// ... 修改数据

// 读操作
q.Lock.RLock()
defer q.Lock.RUnlock()
// ... 读取数据
```

### 条件变量的使用

`Cond`用于协调调度器的阻塞和唤醒：

```go
// 等待
q.Cond.Wait()  // 必须在Lock.Lock()之后调用

// 唤醒一个
q.Cond.Signal()

// 唤醒所有
q.Cond.Broadcast()
```

### 原子操作

对于简单的计数器，使用`atomic.Int32`：

```go
q.PreemptFlag.Add(1)
q.PreemptFlag.Load()
```

---

## 扩展指南

### 必须实现的方法

要创建一个新的队列策略，需要实现`SchedulingQueue`接口的以下方法：

1. **Pop(ctx) (*QueueUnitInfo, error)** - 弹出下一个要调度的任务
2. **AddQueueUnitInfo(ctx, qi) bool** - 添加新任务
3. **UpdateQueueUnitInfo(ctx, old, new)** - 更新任务
4. **DeleteQueueUnitInfo(ctx, qu)** - 删除任务
5. **MoveQueueUnitToUnschedulable(ctx, qi, allocatableChanged)** - 移动到不可调度队列
6. **UpdateQueueCr(ctx, old, new)** - 更新队列CR
7. **AssumeQueueUnitInfo(ctx, qi)** - 假定任务（预留资源）
8. **ForgetQueueUnit(ctx, qu)** - 遗忘任务
9. **GetQueue() *Queue** - 获取队列CR

### 可选覆盖的方法

- `PendingLength()` - 返回准确的pending数量
- `Length()` - 返回准确的总数量
- `Run()` - 启动后台任务
- `Close()` - 清理资源
- `List()` - 如需自定义列表逻辑

### 最佳实践

1. **始终使用BaseQueue的Lock保护并发访问**
2. **在修改队列状态后调用Cond.Broadcast()唤醒调度器**
3. **使用BaseQueue提供的方法处理通用逻辑**，避免重复代码
4. **在Pop方法中调用HandleBlockedQueue或HandleNonBlockedQueue**处理阻塞逻辑
5. **在UpdateQueueUnitInfo中调用HandleResourceRelease和HandleDequeueTransition**
6. **在DeleteQueueUnitInfo中调用CleanupDeletedUnit**
7. **适当设置ResetNextIdxFlag**，在队列状态变化时通知调度器重置遍历位置

---

## 参考实现

- **IntelligentQueue**: 基于优先级阈值的双队列策略，展示了如何使用BaseQueue实现复杂的调度逻辑
- **PriorityQueue**: 优先级队列（未使用BaseQueue，但可作为对比参考）

查看 `intelligentqueue` 包获取完整的使用示例。
