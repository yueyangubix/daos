# Aggregation Barrier设计

## 目录

- [一、现状与问题](#一现状与问题)
- [二、Aggregation Barrier核心设计](#二aggregation-barrier核心设计)
- [三、Barrier状态与清理机制](#三barrier状态与清理机制)
- [四、基于Akey的Barrier存储机制](#四基于akey的barrier存储机制)
- [五、代码实现与集成](#五代码实现与集成)
- [六、Barrier粒度设计](#六barrier粒度设计)
- [七、Dynamic Epoch Threshold设计](#七dynamic-epoch-threshold设计)
- [八、EC聚合触发机制](#八ec聚合触发机制)

## 一、现状与问题

### 1.1 当前机制的本质问题

全局`sc_ec_agg_eph_boundary`用一个时间戳协调三类完全不同特征的操作：

| 操作类型 | Epoch来源 | 目标 | 当前问题 |
|----------|-----------|------|----------|
| 外部读写 | 当前时间戳 | 最新数据 | 必须感知global boundary |
| 内部迁移 | 快照时间戳 | 重建数据 | 必须感知global boundary |
| 内部聚合 | 聚合调度 | 空间回收/校验生成 | 需要设置global boundary |

**根本矛盾**：让不相关的操作相互感知，导致：
- IO路径承担不必要的复杂性
- 聚合逻辑与读写逻辑强耦合
- 大EV问题无法从根本上解决

### 1.2 核心症结

EC满分条数据处理的特殊性：校验数据有效的前提是生成校验数据的原始数据存在。当前通过global boundary保证这一点，但这种保证是粗粒度和不精确的。

## 二、Aggregation Barrier核心设计

### 2.1 设计思想

将Epoch边界从全局级别下沉到dkey级别，让不同dkey的聚合操作可以独立进行：

- **局部性**：每个dkey维护自己的聚合边界
- **依附性**：EC校验数据必须依附于特定的Barrier
- **解耦性**：外部读写和迁移操作无需感知聚合边界

### 2.2 聚合区间语义

`epoch ∈ (barrier_epoch, current_epoch]`

即：Barrier epoch之后的epoch数据可以参与聚合，Barrier epoch本身及之前的数据不参与当前聚合批次。

### 2.3 核心机制

**屏障生成**：
- 客户端满分条写入：直接取分条epoch作为Barrier
- EC聚合时：选择接近当前时间的历史epoch，向shard group发送Barrier记录请求

**屏障清除**：
- 由shard group leader发起
- 需确保校验数据与Barrier的依赖关系
- 确认校验数据不再需要后才清除

**校验数据有效性**：
- 校验数据依附于特定Barrier
- 只有Barrier清除后，对应的校验数据才可被清理

**模块解耦**：
- Aggregation Barrier仅感知Pool Map变化带来的Placement变化
- Shard Group可能新增成员、减少成员；成员可能带老数据、可能不带数据
- AggregationBarrier不感知rebuild/reintegrate/extend
- rebuild/reintegrate/extend尽量做到不感知AggregationBarrier，但流程涉及到的数据清理和迁移可能需要用到。

**聚合逻辑反转**：
- 原逻辑：ec聚合优先于vos聚合。EC聚合完成后，抬升全局sc_ec_agg_eph_boundary，在此边界下做VOS聚合
- 新逻辑：vos聚合优先于ec聚合。vos聚合过程中，发现有必要进行EC聚合后，创建agg barrier，基于agg barrier收集副本数据，编码为校验数据后再用agg barrier作为epoch写入。

## 二、EC聚合判断条件

在VOS聚合过程中（仅在parity shard上），判断是否需要创建新的agg barrier并执行EC聚合。

### 2.1 前提条件

| 条件 | 描述 |
|------|------|
| **条件1** | 只考虑epoch最大的barrier到当前时间这个merge window内的数据recx |
| **条件2** | 只应在parity shard上进行判断 |

### 2.2 触发条件

满足前提条件后，以下任一条件满足时，触发EC聚合流程：

| 条件 | 描述 |
|------|------|
| **条件3** | 扫描到的副本数据能够构成full stripe |
| **条件4** | 零散小块数据的最新写入时间距离当前时间超过设定间隔（如10分钟），推定文件内容已稳定 |

### 2.3 完整判断逻辑

```
IF (条件1 AND 条件2) AND (条件3 OR 条件4)
    THEN 创建barrier触发EC聚合
```

### 2.4 条件说明

**条件1详解**：
- Merge window定义为（max_barrier_epoch, current_time]
- 只扫描这个范围内的数据，避免重复处理已有barrier保护的数据

**条件2详解**：
- 只有parity shard负责判断是否需要生成新parity
- Data shard的VOS聚合正常执行，不进行此判断

**条件3详解**：
- EC聚合的核心前提是必须有完整的数据条带才能生成parity
- 缺少任何data shard的数据都无法生成有效的parity

**条件4详解**：
- 针对无法凑足full stripe的零散小块数据
- 长时间无新写入（超过10分钟）表示数据已稳定
- EC聚合从data shard读取完整数据，生成parity并写入parity shard
- 清理parity shard上的碎片数据，释放空间

## 三、Barrier状态与清理机制

### 3.1 Local Stale与Global Stale

**核心约束**：EC校验数据有效的前提是生成校验数据的原始数据存在。因此，Barrier的清理必须谨慎进行，引入Local Stale和Global Stale两层状态。

**Local Stale（本地失效）**：Barrier被新Barrier覆盖，但尚未确认清理时机

| Shard类型 | Local Stale判断条件 |
|-----------|---------------------|
| 数据片 | 存在更新的Barrier |
| 校验片 | 存在更新的Barrier **且** 有新的校验数据依附于新Barrier |

**Global Stale（全局失效）**：Barrier可以被安全清理的最终确认状态

| Shard类型 | Global Stale判断条件 |
|-----------|---------------------|
| 所有Shard | 向shard group中所有校验片查询，都认为LOCAL_STALE |

**判定逻辑**：
- 无论当前shard是数据片还是校验片，Global Stale的判定标准是一致的
- 需要shard group内**所有parity shard**都判定该barrier为LOCAL_STALE
- 当前shard如果是parity shard，自身已确认为LOCAL_STALE，只需查询其他parity shard
- 当前shard如果是data shard，需要查询所有parity shard

**清理权限**：只有GLOBAL_STALE的Barrier才能执行清理操作。

## 四、基于Akey的Barrier存储机制

### 4.1 Byte Array对象编址扩展

在Byte Array类型对象中，现有的dkey/akey编址规则如下：

| 层级 | 用途 | 编址规则 |
|------|------|----------|
| dkey | 表示索引数据块的偏移 | 64位偏移值，作为dkey直接使用 |
| akey | 标识数据属性 | 固定使用"0"作为akey名 |

针对Aggregation Barrier的存储需求，我们扩展akey的用途：

| 层级 | 用途 | 编址规则 |
|------|------|----------|
| dkey | 保持不变 | 64位偏移值，作为dkey直接使用 |
| akey | 数据属性标识 | "0"表示实际数据；"_agg_barrier_"+epoch表示聚合屏障 |

### 4.2 Barrier Akey命名规范

**命名格式**：

```
_agg_barrier_<epoch>
```

其中：
- 前缀固定为`_agg_barrier_`
- 后缀为Barrier对应的epoch值，采用十六进制表示
- 示例：`_agg_barrier_0x00000000000001F4`表示epoch=500的Barrier

**命名示例**：

| Akey名称 | 含义 |
|----------|------|
| 0 | 实际数据存储的akey |
| _agg_barrier_0x00000000000001F4 | epoch=500的聚合屏障 |
| _agg_barrier_0x00000000000003E8 | epoch=1000的聚合屏障 |

### 4.3 Barrier存储方式

Barrier信息完全通过akey名称表达，不存储额外的value数据。

**原因**：
- Epoch值直接编码在akey名称中
- 状态管理通过不同akey的创建/删除实现
- 简化实现，避免额外存储开销

### 4.4 Barrier生命周期管理

**创建时机**：

1. **客户端满分条写入时**：在EC满分条写入操作中，向iod数组额外添加一个Barrier akey，与原始数据akey位于同一事务中
2. **EC聚合时**：当聚合操作发现需要设置Barrier时，向目标shard group发送Barrier创建请求

**状态转换**：

```
ACTIVE → LOCAL_STALE → GLOBAL_STALE → 清理
   ↓           ↓            ↓
  新Barrier   确认局部失效   确认全局失效
  覆盖        等待确认       可安全清理
```

**更新规则**：

| 触发条件 | 状态变化 | 操作 |
|----------|----------|------|
| 创建新Barrier | ACTIVE | 写入新的Barrier akey |
| 发现新Barrier存在 | LOCAL_STALE | 将旧Barrier标记为失效 |
| 收集所有shard确认 | GLOBAL_STALE | 将Barrier标记为可清理 |
| 超时且无依赖 | 清理 | 删除Barrier akey |

### 4.5 数据访问与Barrier感知

**读路径**：

- 读取数据时，透明地跳过Barrier akey
- Barrier akey不参与正常的数据读取逻辑

**写路径**：

- 正常数据写入使用akey="0"
- Barrier写入使用akey="_agg_barrier_"+epoch
- 满分条写入时，Barrier与数据在同一事务中完成

**聚合路径**：

- 聚合操作遍历dkey下的所有akey
- 识别Barrier akey以获取Barrier信息
- 根据Barrier epoch确定聚合区间

### 4.6 与现有机制的兼容性

**优势**：

- 复用现有的dkey/akey遍历逻辑
- 无需修改vos_krec_df等核心数据结构
- Barrier存储与数据存储使用相同的空间管理机制
- 支持事务原子性保证

**注意事项**：

- 需在akey过滤逻辑中排除Barrier akey
- 需在数据校验时忽略Barrier akey
- 需在空间统计时排除Barrier akey

## 六、Barrier粒度设计

### 6.1 Chunk粒度vsStripe粒度

**设计选择**：采用Chunk粒度（dkey维度），而非Stripe粒度。

**Stripe粒度的复杂性**：

如果将Barrier粒度做到Stripe级别，会面临以下问题：

1. **VOS聚合不感知Stripe概念**：VOS在data shard内部将0-256KB视为连续空间，无法区分不同stripe的数据边界

2. **数据分布示例**（EC4+2，chunk_size=1MB）：
   ```
   VOS视角（连续空间）：
   |---0-64KB---|---64-128KB---|---128-192KB---|---192-256KB---|
        ↑stripe 0   ↑stripe 1      ↑stripe 2       ↑stripe 3
   ```

3. **实现复杂度**：若要做到stripe粒度，需要：
   - 修改VOS聚合逻辑感知EC stripe概念
   - 将data shard切分为多个独立空间处理
   - 大幅度增加实现复杂度

**Chunk粒度的优势**：

1. 复用现有的VOS聚合逻辑，无需修改底层遍历机制
2. 保持数据访问路径的一致性
3. 实现简洁，风险可控

### 6.2 多Stripe的Epoch差异问题

**问题场景**：

一个chunk内的4个stripe可能有不同的epoch：

| Stripe | as_hi_epoch |
|--------|-------------|
| stripe 0 | 100 |
| stripe 1 | 200 |
| stripe 2 | 150 |
| stripe 3 | 180 |

**解决方案**：

使用chunk内所有stripe的**最大epoch**作为barrier epoch，而非为每个stripe单独创建barrier。

## 七、Dynamic Epoch Threshold设计

### 7.1 设计背景

**风险场景**：

```
时间线：
1. epoch = 100: 数据写入
2. epoch = 200: VOS聚合执行，将epoch=100的数据合并到更高epoch
3. epoch = 250: EC聚合发现epoch=100的满stripe，尝试创建barrier

问题：
- 创建epoch=100的barrier
- barrier存在，parity存在
- 但epoch=100的data可能已被VOS聚合合并
- parity保护的前提被破坏！
```

**核心原则**：

> **新建Barrier的Epoch不能小于VOS聚合可能达到的最大Epoch。**

### 7.2 双Threshold设计

为应对网络延迟、DTX冲突和超时等情况，采用双Threshold设计：

| Threshold | 默认值 | 作用 |
|-----------|--------|------|
| VOS聚合上限 | now - 240s | 表示VOS聚合已经完成的Epoch范围 |
| AGG Barrier下限 | now - 120s | 创建Barrier的最小Epoch要求 |
| 时间间隔 | 120s | 应对网络延迟、DTX冲突、超时处理 |

### 7.3 设计理由

1. **使用过去的Epoch**：避免与当前活跃的DTX操作冲突
2. **now - 240s**：确保VOS聚合有足够时间完成，历史数据已被安全聚合
3. **now - 120s**：留出时间间隔，允许网络延迟和超时处理
4. **时间间隔=120s**：足够处理可能的重试和超时场景

### 7.4 Barrier创建判断逻辑

```
查询满stripe的epoch：
  |
  v
如果 epoch <= vos_agg_upper_bound (now - 240s):
  -> 该epoch的data已被VOS聚合，无需创建barrier
  -> barrier无效，不做处理
  |
  v
如果 vos_agg_upper_bound < epoch <= agg_barrier_lower_bound (now - 120s):
  -> 处于等待区间，可能有网络延迟或DTX未完成
  -> 等待下次检查
  |
  v
如果 epoch > agg_barrier_lower_bound (now - 120s):
  -> 满足创建barrier的条件
  -> 可以创建barrier
```

### 7.5 为什么要与当前时间保持距离

**考虑因素**：

1. **DTX冲突避免**：当前时间附近的epoch可能处于活跃的分布式事务中，创建barrier可能与DTX操作产生冲突
2. **网络延迟容忍**：网络中的写请求可能有延迟，需要等待这些"在路上"的数据被处理
3. **超时处理**：分布式系统中的操作可能超时重试，需要足够的时间间隔确保一致性
4. **历史数据稳定性**：过去的epoch意味着相关操作已完成，数据状态已稳定

**设计权衡**：

- 240s和120s的间隔是经验值，平衡了系统响应性和一致性需求
- 可根据实际运行情况调整这些参数

## 八、EC聚合触发机制

### 8.1 发布-订阅模式抽象

Aggregation Barrier机制采用发布-订阅模式实现VOS聚合与EC聚合的解耦：

```
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│  VOS聚合        │  发布   │  Aggregation    │  订阅   │  EC聚合         │
│  (发布者)       │ ──────→ │  Barrier        │ ──────→ │  (消费者)       │
│                 │         │  机制           │         │                 │
└─────────────────┘         └─────────────────┘         └─────────────────┘
```

**模式特点**：
- **发布者**：VOS聚合，负责创建Barrier并发布EC聚合任务
- **订阅者**：每个ParityShard，独立订阅并处理自己需要的EC聚合任务
- **解耦**：VOS不感知EC的具体逻辑，EC按自己的节奏处理

### 8.2 触发流程

**整体流程**：

```
1. VOS聚合扫描（仅在parity shard上）
      ↓
2. 判断满足EC聚合条件（满足前提条件1-2，且触发条件3或4）
      ↓
3. 向shard group发送创建Barrier RPC（需所有shard成功）
      ↓
4. Barrier创建成功后，发送create_ec_parity RPC到所有parity shard
      ↓
5. Parity shard收到create_ec_parity后：
      创建ULT异步执行EC聚合
      ULT创建成功即可返回RPC成功
      无需等待ULT执行完成
      ↓
6. EC聚合ULT异步执行
      （失败不影响数据一致性，只是空间浪费）
```

### 8.3 各Parity Shard独立处理

**设计决策**：不采用Parity Leader模式，由每个Parity Shard独立处理自己的EC聚合任务。

**优点**：
- 无单点故障
- 无需Leader选举
- 并行处理，提高效率
- 实现简单，风险低

**处理流程**：

```
Parity shard收到create_ec_parity RPC：
  |
  v
创建ULT，ULT任务：
  1. 基于Barrier epoch读取data shard完整数据
  2. 生成parity
  3. 写入本地parity shard
  |
  v
ULT完成（成功或失败）
```

### 8.4 RPC与ULT的异常处理

**create_agg_barrier RPC**：
- 需要确保shard group中所有shard成功创建Barrier
- 失败时整个RPC失败，需要重试或回滚

**create_ec_parity RPC**：
- 与create_agg_barrier保持一致的异常处理机制
- 需要确保发送到所有parity shard
- 失败时重试

**EC聚合ULT**：
- 不重试
- 只记录日志
- 失败不影响数据一致性，只是空间浪费

**设计原则**：
- Barrier创建：严格保证
- RPC发送：尽力保证
- EC聚合：尽力而为，失败不影响数据
