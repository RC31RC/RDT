# Redis Distributed Toolkit（RDT）

面向 **JDK 21、Spring Boot 4、Redisson 4.7** 的分布式基础能力工具包。当前为开发版本，API、Redis 状态和调用方同步更新，不保留旧构造器、双写或历史键格式回退。

- **使用方法**集中在本 README。
- **实现原理、状态机、可靠性边界和面试问题**集中在 [原理与设计](docs/原理与设计.md)。
- 配置和代码示例只使用本机地址；不会自动创建数据库表。

## 1. 模块与选型

| 模块 | 职责 | 是否需要业务数据库 |
| --- | --- | --- |
| `rdt-core` | 公共契约、消息信封、策略、一致性 SPI，以及锁/队列/缓存/ID/调度的 Redisson 实现（`io.github.rdt.redisson` 包） | 否；不包含 JDBC 或 Spring 生产依赖 |
| `rdt-spring-boot-starter` | 自动配置、事务同步、注解、组件生命周期、指标 | 否；只使用 Spring 事务抽象 |
| `rdt-consistency-jdbc-starter` | 可选 Outbox、Inbox、JDBC 死信、清理任务及 SQL 方言 | 是；使用应用自己的 DataSource 和事务管理器 |
| `rdt-console` | 独立管理端、实例/任务/Stream/死信管理、告警、异常归档 | 默认否 |

普通应用依赖 `rdt-spring-boot-starter`，传递引入 core。需要数据库一致性才额外引入 JDBC starter。独立 Console 不参与业务消费，不替业务实例执行任务。

### 能力选择

- 定时任务：类级 `@RdtScheduled` + `RdtScheduledTask`。
- 消费者：类级 `@RdtQueueListener` + `RdtMessageConsumer<T>`。
- 消息生产：显式 `RdtMessageSendManager.send / sendAfterCommit / sendOutbox`，每次选择投递语义。
- 同步方法互斥：`@RdtLock` 建立作用域，`DistributedLockManager.lock` 指定资源。
- 跨方法、跨回调 owner：显式 `RedisOwnerLeaseService`，不要把线程绑定的锁带入异步回调。
- 数据库消费去重：Inbox；Redis 幂等模板不是数据库原子性替代品。
- JVM 本地缓存：由应用选型；NnmBot 保持 cache2k，不自动打开 RDT 本地缓存。

## 2. 最小接入

### 2.1 Maven

```xml
<dependency>
    <groupId>io.github.rdt</groupId>
    <artifactId>rdt-spring-boot-starter</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 2.2 单客户端配置

```yaml
rdt:
  namespace: orders:local
  instance-id: orders-local-1
  redis:
    runtime:
      config: |
        threads: 2
        nettyThreads: 2
        singleServerConfig:
          address: "redis://127.0.0.1:6379"
          database: 0
          connectionMinimumIdleSize: 1
          connectionPoolSize: 12
          subscriptionConnectionMinimumIdleSize: 1
          subscriptionConnectionPoolSize: 4
```

1. namespace 是应用/环境边界；协作实例必须相同。任务 ID、topic 和 group 使用逻辑名称。
2. 并行运行的 instance-id 必须唯一；重启保持稳定，可靠广播才能接续原消费组。
3. 不配置 instance-id 可生成临时身份，但不能用于可靠广播。
4. `config` 是完整原生 Redisson YAML，支持单机、Sentinel、Cluster、ACL、TLS、连接池、超时和 codec；RDT 不重写它。密码含特殊字符时按 YAML 字符串正确转义。
5. 也可使用 `rdt.redis.runtime.file: classpath:redisson-runtime.yml` 或 `file:/absolute/path/redisson-runtime.yml`。同一用途的 config/file 二选一。空配置、无效配置或不可读文件启动失败。
6. 没有声明配置时使用本机小连接池默认值；RDT 不执行任何数据库 DDL。
7. `rdt.enabled=false` 关闭 RDT 自建客户端和组件；不替应用关闭其自行管理的连接。

### 2.3 runtime/cache 两个用途

```yaml
rdt:
  redis:
    runtime:
      file: classpath:redisson-runtime.yml
    cache:
      file: classpath:redisson-cache.yml
```

- 不再配置 Bean 名称。RDT 创建并关闭用途固定的 runtime/cache 客户端；高级应用可用同名 `rdtRuntimeRedis` / `rdtCacheRedis` Bean 替换创建逻辑。
- 未配置 cache 时复用 runtime；显式配置 cache 时使用独立客户端。CacheService 数据、回源锁和失效通知均使用 cache。
- runtime 用于锁、Stream、调度、owner 和运行目录；缓存按淘汰策略选择独立 Redis 实例。
- 两份配置都保留 Redisson 原生配置能力；Cluster 只能 DB0。
- 同一 Redis 的 DB0/1/2 只是逻辑键分离，不能隔离内存、故障或淘汰策略。
- Sa-Token 的 RedisConnectionFactory/RedisTemplate 由业务单独装配；NnmBot 的会话使用独立连接池和 `spring.data.redis.*` 配置。

## 3. 消息发送：显式选择时机

业务定义自己的消息类型，继承 `MessageDescriptor<T>`。它包含类型化载荷和元数据，topic/mode 与消费者策略由本地代码定义：

```java
public final class OrderCreated extends MessageDescriptor<OrderPayload> {
    public OrderCreated() { } // 启动注册与 JSON 解码需要无参构造器
    public OrderCreated(OrderPayload payload) { super(payload); }
    public OrderCreated(OrderPayload payload, MessageMetadata metadata) { super(payload, metadata); }

    @Override
    public String topic() { return "order-events"; }

    @Override
    public ConsumerDescriptor consumer(String group) {
        return new ConsumerDescriptor() {
            @Override
            public int concurrency() { return 4; }
            @Override
            public InboxMode inbox() { return InboxMode.DISABLED; }
            @Override
            public QueueOptions options() {
                return QueueOptions.builder().maxAttempts(5)
                        .retryDelay(Duration.ofSeconds(1)).retryMultiplier(2)
                        .maxRetryDelay(Duration.ofMinutes(1)).build();
            }
        };
    }
}
```

- `payload()` 读取业务数据；`metadata()` 读取 messageId、createdAt、key、orderingKey、headers。
- 默认元数据生成 UUID 和时间；需要追踪信息时构造 `MessageMetadata` 传入。正常重试和 Outbox 保留同一信封 ID。
- `mode()` 默认 STREAM；可靠广播覆盖为 RELIABLE_BROADCAST，在线广播覆盖为 BROADCAST。
- `consumer(group)` 返回该组的本地 ConsumerDescriptor；同消息不同组可有不同并发、Inbox 和重试策略。没有覆盖时继承全局默认值。
- topic/mode/consumer 不得依赖 payload 或收到的 headers。注册器用无参实例读取本地策略，不接受消息携带的策略。

```java
@Resource
private RdtMessageSendManager messages;

public void notifyWorker(OrderPayload payload) {
    PublishReceipt receipt = messages.send(new OrderCreated(payload));
    // messageId 是稳定信封 ID，不是 Redis Stream entry ID。
}
```

| API | 触发时机 | 事务要求 | 失败语义 |
| --- | --- | --- | --- |
| `send(descriptor)` | 立即发 | 无 | 异常直接抛出；后续业务回滚不撤销已发消息 |
| `sendAfterCommit(descriptor)` | 当前事务成功提交后 | 实际事务与同步器 | 尽力而为；提交后失败只记 error，回滚不发送 |
| `sendOutbox(descriptor)` | 业务事务写 Outbox，后台中继 | 可写应用事务与 OutboxStore | 至少一次、不保序；持久化重试，超限 DEAD |

### 3.1 DIRECT

```java
messages.send(new OrderCreated(payload));
```

投递方式由业务描述符的 mode() 决定；处于事务中仍立即发送，不隐式改变语义。

### 3.2 AFTER_COMMIT

```java
@Transactional
public void saveAndNotify(Order order) {
    repository.save(order);
    messages.sendAfterCommit(new OrderCreated(new OrderPayload(order.getId())));
}
```

回滚不发；没有事务直接报错。提交到发送之间崩溃或 Redis 发送失败都可能遗漏，适合可以重建的通知。

### 3.3 OUTBOX

```java
@Transactional
public void createOrder(Order order) {
    repository.save(order);
    messages.sendOutbox(new OrderCreated(new OrderPayload(order.getId())));
}
```

- 第 5 节的 JDBC 模块须引入、启用并手工建表；缺模块、无事务或只读事务都明确报错。
- 业务数据与 Outbox 同提交、同回滚；最终投递是至少一次，消费者必须幂等。
- 不接受 BROADCAST 或 orderingKey；需要顺序使用底层 sendOrdered。
- 返回收据分别是 SENT、AFTER_COMMIT_REGISTERED、OUTBOX_STORED，后两者不表示消息已到 Redis。

### 3.4 信封与底层发送 API

`Message` 保存 `id/payload/createdAt/schemaVersion/key/orderingKey/headers`。payload 是 JSON；当前 schemaVersion 必须显式为 `1`，缺失或未知版本拒绝。

```java
Message message = Message.create(event, jsonCodec);
queue.sendMessage("order-events", message, QueueMode.STREAM); // 保留原始 ID
```

- `send`、`sendMessage`、`broadcast`、`reliableBroadcast` 是立即传输 API，不提供业务事务保证。
- `MessageDescriptor.envelope(jsonCodec)` 生成完整信封；接收时恢复运输层元数据，收到的 JSON 不能替换消息 ID。
- payload 使用受信任的业务 DTO；不从外部输入决定任意 Java 类型。
- 正常重试和 Outbox 中继保留 ID；人工死信重放生成新 ID，并在 headers 记录 `originalMessageId`。
- headers 最多 32 项。人工补充追踪字段时给重放来源字段留空间。

## 4. 消费者、模式、并发与重试

### 4.1 声明一个同步消费者

```java
@RdtQueueListener(topic = "order-events", group = "order-workers")
public class OrderCreatedConsumer implements RdtMessageConsumer<OrderCreated> {
    @Resource
    private OrderService orders;

    @Override
    public void consume(OrderCreated event) {
        orders.apply(event.payload(), event.metadata().messageId());
    }
}
```

- 注解本身含 `@Component`，不要重复注册同一个消费者 Bean。
- 每个类表达一个订阅；不使用方法级监听注解。
- 泛型必须是具体的业务 MessageDescriptor 子类；用 payload()/metadata() 读取数据与信封。注解只声明 topic/group，topic 必须匹配描述符；监听器标识为 topic:group。
- consume 同步返回后才允许 ACK；业务失败直接抛异常，不要捕获后返回成功。
- 不要在 consume 内仅提交异步线程、异步事件或 `CompletableFuture` 就返回。
- 注册器拒绝声明 `@Async` 的消费者/任务，但不能自动识别手工丢入线程池的逻辑。

### 4.2 三种模式及并发含义

| QueueMode | 投递契约 | `concurrency` 的含义 |
| --- | --- | --- |
| `STREAM` | 同 group 竞争消费，持久化 Pending，至少一次 | 普通订阅一个批量拉取循环，本地最多 N 个同步 handler，并非 N 条阻塞连接 |
| `RELIABLE_BROADCAST` | 每个稳定实例一个消费组，所有实例各消费；离线恢复受保留策略限制 | 每实例普通订阅一个拉取循环，本地最多 N 个 handler |
| `BROADCAST` | Topic 在线通知；离线、容量拒绝或进程退出均可能丢失 | 每订阅最多 N 个本地回调，同时受 JVM 总 handler 槽限制 |

可靠广播实际 group 为 **`配置 group:规范化 instanceId`**；Inbox 使用同一计算方式，不能用未经改写的 group，也不能只按 messageId 去重。

### 4.3 重试与死信

**是否重试与怎样退避是两个独立判断。** 队列和调度共同使用 `RetryClassifier`；只有分类器允许、失败预算未耗尽、退避大于零时才可能重试。调度还要求 `failureMode = RETRY`。

- RDT 默认仅把明确的 Redis 操作失败、Redis 超时和容量限流错误码视为可重试基础设施故障。未知异常、配置错误、序列化错误不自动重试。
- 应用可以声明一个 `RetryClassifier` Bean，替换默认分类器；RDT 不认识也不依赖应用的业务异常。
- NnmBot 的实现为 `failure -> failure instanceof SystemException`：只有应用系统异常及其子类可重试；`BusinessException`、未知异常均不重试。不沿 cause 链猜测，以免把包有系统错误的业务拒绝误判成可重试。
- 永久失败并非吞掉或当成功：消息保存死信后才 ACK；任务归档 FAILED，后续正常周期不因此停止。
- 消费/任务同步调用链不要“每层 catch + 日志 + 换类型”。应原样上抛，由框架执行边界记录一次；只有明确的协议/存储边界才转换异常。
- 不要把所有未知异常包装成 `SystemException` 来绕过分类。永久校验失败应表达为业务异常，已识别的临时系统故障才使用系统异常。

```java
@Configuration(proxyBeanMethods = false)
public class RetryConfiguration {
    @Bean
    public RetryClassifier rdtRetryClassifier() {
        return failure -> failure instanceof SystemException;
    }
}
```

上面的 `SystemException` 是**接入应用自有类型**，示例配置写在应用中，不写进 RDT。`DIRECT` 的调用失败、`AFTER_COMMIT` 的尽力投递和 Outbox 中继属于发送阶段，不是此业务回调分类器的职责。进程崩溃、Pending 恢复、租约和派发拒绝也不伪装成业务失败。

```java
// OrderCreated.consumer(group) 中配置退避；消费者注解始终只有身份。
@RdtQueueListener(topic = "order-events", group = "payment-workers")
public class PaymentConsumer implements RdtMessageConsumer<OrderCreated> {
    @Override
    public void consume(OrderCreated event) {
        paymentService.apply(event.payload());
    }
}
```

- 只有已观测到的处理失败消耗 failureCount；拉取、派发和进程崩溃不是业务失败次数。
- deliveryCount 记录投递观测，与重试预算分开。
- `visibilityTimeout` 是崩溃恢复的 Pending 空闲阈值，不是每条消息的可见性租约。
- 显式重试由到期索引控制，不必等满整个崩溃恢复窗口。
- Pending 恢复先筛选未到期重试，再显式 claim；不先认领再跳过来制造空转。
- 达到失败预算，先保存死信，再 ACK；保存失败不能当消费成功。
- 在线广播也有本地有界延迟重试；退避时释放 handler 槽，但不因此获得离线可靠性。
- 关闭订阅后，其尚未触发的广播重试不再调用旧业务处理器。

### 4.4 延迟消息

```java
queue.sendDelayed("order-timeouts", new OrderTimeout(orderId), Duration.ofMinutes(15));
```

- 写持久化到期索引，后台发现到期项后用 Lua 原子转入 Stream。
- 消费者仍使用 `STREAM`，不需要独立的 DELAYED 模式。
- 进程退出后其他同 namespace 的队列实例仍可继续转移；时间精度受轮询影响。
- 它是 Redis 延迟投递，不是事务 Outbox，也不是业务数据库超时订单扫描的替代品。

### 4.5 分区有序消息

```java
queue.sendOrdered("account-events", accountId, new AccountEvent(payload), 16);
```

AccountEvent 同样继承 MessageDescriptor，在 consumer(group) 中覆盖 orderedPartitions() 为 16、concurrency() 为 4。对应订阅：

```java
@RdtQueueListener(topic = "account-events", group = "account-workers")
public class AccountConsumer implements RdtMessageConsumer<AccountEvent> {
    @Override
    public void consume(AccountEvent event) {
        accountService.apply(event);
    }
}
```

- orderingKey 经 CRC32 映射固定分区；同一分区串行，分区之间可并行。
- 分区数默认 16，允许 1–128；生产者和消费者必须一致，不在线随意改变已有主题的分区数。
- 失败头消息的退避会阻塞该分区后续消息，直到成功或转死信。
- ordered 路径的 claim `minIdle=0` 依赖分区锁；不能照搬到普通并发消费。
- 不保证全局顺序，也不能倒转数据库/外部系统已经发生的副作用。

## 5. 可选数据库一致性模块

### 5.1 依赖与前置条件

```xml
<dependency>
    <groupId>io.github.rdt</groupId>
    <artifactId>rdt-consistency-jdbc-starter</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

1. 提供应用业务库的 DataSource 和 PlatformTransactionManager，以及对应 JDBC 驱动。
2. JPA 应用直接使用自己的 JpaTransactionManager；不要另建 DataSourceTransactionManager。
3. 多 DataSource/事务管理器的应用应在应用配置中消除歧义，并确保 RDT 选中业务写入所用的同一套资源。
4. 选择并手工执行 `rdt-consistency-jdbc-starter/src/main/resources/sql/{postgresql,mysql,h2}/outbox.sql` 与 `inbox.sql`。
5. 有 schema 时给模板 DDL 的表名加上对应 schema 前缀；运行时设置 `rdt.consistency.schema`。配置不会创建 schema、改 search_path 或自动执行 SQL。
6. PostgreSQL、MySQL 8.0.17+ 和 H2 使用不同方言。MySQL 表显式使用 InnoDB、utf8mb4_0900_bin 保持事务和身份大小写语义，完整信封使用 LONGTEXT。H2 用于测试；其 Inbox SQL 还创建有界的插入锁条带辅助表，不要漏执行脚本尾部。

```yaml
rdt:
  consistency:
    enabled: true
    schema: nnmbot
    inbox-enabled: false
    relay-interval: 500ms
    batch-size: 16
    claim-lease: 60s
    max-attempts: 10
    retry-delay: 1s
    max-retry-delay: 1h
    retention: 7d
    cleanup-time: "03:30"
    cleanup-jitter: 5m
    cleanup-batch-size: 1000
    cleanup-limit: 10000
    cleanup-pause: 100ms
```

`enabled` 默认 false；仅增加依赖不启用任何表写入、中继或清理。未引入模块时普通消息行为不变；显式请求 OUTBOX 或 Inbox ENABLED 不会静默降级。

### 5.2 开启 Inbox

```java
// OrderCreated.consumer(group).inbox() 返回 InboxMode.ENABLED。
@RdtQueueListener(topic = "order-events", group = "order-workers")
public class TransactionalOrderConsumer implements RdtMessageConsumer<OrderCreated> {
    @Resource
    private OrderRepository orders;

    @Override
    public void consume(OrderCreated event) {
        orders.applyEvent(event);
    }
}
```

- `INHERIT`：跟随 guard 的默认值；JDBC guard 默认值来自 `rdt.consistency.inbox-enabled`。
- `ENABLED`：启动时必须存在支持 Inbox 的 ConsumerGuard，缺失或能力不符立即失败。
- `DISABLED`：不使用 Inbox，即使全局默认打开也保持关闭。
- 仅支持 STREAM / RELIABLE_BROADCAST；不对在线 Topic 伪装可靠事务语义。
- 唯一键为 `(namespace, message_id, consumer_group)`。
- guard 断言入口没有外层事务，再用应用事务管理器、REQUIRED 开始最外层事务。
- 先注册 Inbox；重复就正常返回；首次才同步执行业务；业务抛异常时 Inbox 与业务共同回滚。
- guard 返回表示事务提交已完成，上层才 ACK。不要在业务内部另开 REQUIRES_NEW 来绕开原子性边界。
- Redis/HTTP/文件等外部副作用不能随数据库事务回滚；仍需业务幂等或下游 fencing。
- 去重只覆盖保留窗口内的同 messageId/group；数据库业务唯一约束仍是长期防线。

### 5.3 Outbox 生命周期与清理

- `PENDING -> CLAIMED -> PUBLISHED`；发送失败重排 PENDING，超过预算转 DEAD。
- 认领使用短事务与 `FOR UPDATE SKIP LOCKED`；提交后才开始 Redis I/O。
- 超时 CLAIMED 在同一认领事务内直接改绑新 owner/token，不经过“先释放回待发送”的窗口。
- 重试时间、租约和清理截止时间由数据库时钟加减时长计算，应用不传绝对到期时间。
- 每实例一个单线程守护中继，默认 500ms 一轮；batch-size 是单轮预算，逐条认领、发送、及时标记，避免慢消息占掉整批租约。
- draining、失去节点身份或准入暂停时不再发送新记录；已经开始的网络调用不能强制撤回。
- 发送成功、标记前崩溃会再次发送，messageId 保持不变，这是至少一次而非 exactly-once。
- 每日默认按应用进程本地时区 03:30 加 0–5 分钟抖动；取得 namespace 内分布式锁后分批清理并让出数据库连接。
- 只删超过 7 天的 PUBLISHED Outbox 和 Inbox；不删除 PENDING、CLAIMED、DEAD，也不跨 namespace 清理。
- 使用自定义 Store 时自动配置不强绑 JDBC 实现；自定义存储的清理由实现方负责。

### 5.4 可选 JDBC 死信

`rdt_failed_message` 是可选的 JDBC 死信表：消息发生不可重试异常、确认失败达到预算上限或信封损坏时才保存最终失败诊断。它不存正常消息，也不承载等待中的指数退避重试。默认死信保存在 Redis；NnmBot 当前未启用此表。

使用 JDBC 死信需引入 `rdt-consistency-jdbc-starter`，手工执行 `src/main/resources/sql/{postgresql,mysql,h2}/dead-letter.sql` 中所选数据库的脚本，再设置：

```yaml
rdt:
  consistency:
    enabled: true
    jdbc-dead-letters: true
```

它替换默认 Redis FailedMessageStore，使用独立短事务保存失败诊断。现有 Console 死信列表/重放读取 Redis，不自动查询 JDBC 死信；不要配置后误以为 Console 能管理数据库中的记录。

### 5.5 尚未提供的能力

- Console 的 Outbox 分页、统计、DEAD 人工重发接口本轮仍延后；当前没有对应 UI 入口，也没有伪 503 接口。
- 不提供 Outbox 保序、跨库事务、分布式 exactly-once 或业务线程自动中止。
- MySQL 的判重受 JDBC `CLIENT_FOUND_ROWS` 影响，因此增加 claim marker 核验，而不是只依赖不稳定的影响行数；详见原理文档。

## 6. 集群调度

### 6.1 类级任务

```java
@RdtScheduled(id = "refresh-catalog-v1", failureMode = ScheduleFailureMode.RETRY)
public class RefreshCatalogTask implements RdtScheduledTask {
    @Override
    public TaskDescriptor descriptor() {
        return new RefreshCatalogDescriptor();
    }

    @Override
    public void execute() { catalogService.refresh(); }
}

public final class RefreshCatalogDescriptor extends TaskDescriptor {
    @Override
    public String displayName() { return "刷新目录"; }
    @Override
    public ScheduleSpec schedule() {
        return new ScheduleSpec.FixedDelay(Duration.ofMinutes(5), Duration.ofSeconds(30));
    }
    @Override
    public ScheduleOptions options() {
        return ScheduleOptions.builder().maxAttempts(3).retryDelay(Duration.ofSeconds(10)).build();
    }
}
```

- 注解只保留 id、route、failureMode；默认 HEALTH_FIRST/CONTINUE。TaskDescriptor 提供显示名、ScheduleSpec 和 ScheduleOptions，应用可以用类型化配置构建它；注册前一次性验证。
- ScheduleSpec 用 FixedRate、FixedDelay、Cron 三种明确类型，注解自身注册 Component。
- fixed-delay 从逻辑执行最终完成后计算下一次；fixed-rate 合并错过的周期，不补跑无限积压。
- cron 支持项目规范化后的五字段及 Spring 六字段表达式，不是 Quartz 七字段协议。时区通过 zone 明确指定。
- 同一 taskId 的共享定义必须一致；FAIL 默认拒绝定义冲突，更新策略应显式声明。
- displayName 只作展示，不参与路由身份；业务标识使用稳定 taskId。
- FIRE_ONCE 合并错过触发；SKIP 才要求正的 misfireThreshold。
- `CONTINUE` 表示失败后继续后续周期；`RETRY` 仅在 `RetryClassifier` 允许时按本次执行的失败预算重试。业务异常/未知异常即使配置 RETRY 也不重试。

### 6.2 路由与显式 fencing 检查点

```java
Subscription handle = scheduler.registerContextual(
        "reconcile-v1", "对账", new ScheduleSpec.FixedDelay(Duration.ofMinutes(5), Duration.ZERO),
        ScheduleOptions.builder().route(JobRoute.HEALTH_FIRST).build(),
        context -> {
            context.assertLease();
            reconcileService.prepare();
            context.assertLease();
            return reconcileService.apply();
        });
// 模块退出时 handle.close() 取消本机注册；不删除共享任务历史。
```

- `HEALTH_FIRST` 优先较空闲的合格节点；`RANDOM` 随机；`SPECIFIED` 需要 targetInstanceId。
- worker 从有 TTL、无管理分页截断的目录选择；目录无候选时进入 WAITING 和带抖动退避，不静默停摆。
- 节点级 reservation 是 Redis token→expireAt 账本；本地 nodeSlots 是 JVM 硬保护；每 job 另有软上限。
- draining 拒绝新派发和新接单，但已有执行继续续租。
- 执行通过 executionId、dispatchToken、runId、租约期限 CAS 防旧 worker 覆盖新属主。
- 续租失败会 fence 当前 attempt；assertLease 是协作检查点，不能终止已经进入外部系统的副作用。
- 过期完成归档为 LEASE_LOST，不当作成功或业务失败，也不覆写新 execution。
- 详细合法迁移、每处 owns 校验和恢复顺序见唯一原理文档的“调度状态机”章节。

### 6.3 外部化单组件覆盖

```yaml
rdt:
  components:
    listeners:
      order-created-listener:
        concurrency: 2
        max-attempts: 5
        retry-delay: 2s
    jobs:
      refresh-catalog-v1:
        max-attempts: 4
        retry-delay: 30s
        route: SPECIFIED
        target-instance-id: orders-local-1
```

覆盖项只作用于对应 listener ID / taskId。未指定监听器 id 时默认 `destination:group`；不要用展示名匹配配置。

## 7. 分布式锁

### 7.1 显式句柄

```java
try (LockHandle ignored = locks.tryLock("order:" + orderId,
        new LockOptions(LockType.REENTRANT, Duration.ofMillis(100), Duration.ZERO)).orElseThrow()) {
    orderService.update(orderId);
}
```

- 锁必须在取得它的线程释放；同步范围内用 try-with-resources，不跨 WebSocket/线程池回调持有。
- `tryLock(key)` 和 `lockManager.lock(key)` 使用生效默认策略；显式 LockOptions 不受默认覆盖影响。
- `leaseTime=0` 启用 watchdog，正值是固定租约且不自动续期；正值配置启动时打印 WARN。
- 支持 REENTRANT、FAIR、READ、WRITE；公平策略可能降低吞吐，不默认切换。

### 7.2 方法作用域内按需加锁

```java
@RdtLock
public void updateOrder(long id) {
    lockManager.lock("order:" + id);
    repository.update(id);
}
```

- 注解只建立作用域，不读取业务锁 key；锁资源在方法内部显式添加。
- 不需要注解时可 `try (LockScope scope = lockManager.beginScope()) { scope.lock(key); ... }`。
- 经 Spring 代理的 public、非 final 方法才触发切面；self-invocation 不生效。
- 当前事务适配器在实际加锁时发现已存在事务，会将已取得锁的释放延迟到该事务完成；不应把它理解成任意外层事务图上的锁累积管理器。
- 无事务时同步方法退出释放；异步线程不继承作用域。不要用 ThreadLocal 传播业务身份、锁或事务。
- 锁不能替代数据库唯一约束、事务、消息幂等和外部 fencing。

### 7.3 信号量

- `locks.trySemaphore(key, permits, wait)` 是基础 RSemaphore，不具备持有者崩溃自动回收。
- 需要固定租约并发限制时使用第 11 节 `tryConcurrency`；它同样不会自动续租。

## 8. 跨回调 owner lease

`io.github.rdt.redisson.lease.RedisOwnerLeaseService` 是显式编程 API，适用于长连接 owner：

```java
var lease = ownerLeases.acquire("socket:" + accountId, instanceId, Duration.ofSeconds(30)).orElseThrow();
// 连接存活期间显式续租；失败必须停止以此身份进行业务副作用。
boolean renewed = ownerLeases.renew(lease, Duration.ofSeconds(30));
boolean held = ownerLeases.isHeld(lease);
long fence = lease.fencingVersion();
// 连接生命周期终止，比较 token 释放，不会删除后来接管者的 owner。
ownerLeases.release(lease);
```

- lease 是可显式跨回调传递的值，不依赖当前线程。
- 调用方维护续租、关闭连接和停止发送的生命周期；Redis key 到期不等于远端 socket 自动关闭。
- fencingVersion 应由实际业务写入方/下游校验，否则只能保护 owner 元数据，不能阻止僵尸进程副作用。

## 9. 缓存与 region

### 9.1 编程式

```java
CacheService products = cache.region("products");
Product product = products.getOrLoad("product:" + id, Product.class,
        Duration.ofMinutes(10), true, () -> repository.findProduct(id));
products.put("product:" + id, product, Duration.ofMinutes(10));
products.evict("product:" + id);
```

```yaml
rdt:
  cache:
    local-enabled: false
    regions:
      products:
        ttl: 10m
        null-ttl: 15s
        local-enabled: false
```

- region 需预先配置，或通过 `region(name, CachePolicy)` 创建；同名 region 不允许不同策略。
- `CacheEntry.Hit` 可携带缓存的 null；Miss 表示无条目，不能只用返回 null 区分。
- `getAll` 使用本地命中并批量读缺失项；`putAll` 批量写值和逐 key 失效。
- `CacheService` 只管理可丢弃的 JSON bucket，不承载共享事实、字段 Map 或计数器。
- 失效通知按 region 隔离；只有该 region 存在 L1 订阅者时才发布，写节点本身无需开启 L1。
- 全区域清理仅通过管理端 `POST /cache/clear` 执行：需要 ADMIN、namespace、显式 Redis role、连接 revision 和 `confirmed=true`。内部使用 SCAN，每 500 个键分批 UNLINK，包含子 region，但保留共享状态、Map 和计数器。
- 默认本地缓存关闭；开启后 Pub/Sub 失效可能漏通知，只能由本地 TTL 兜底，允许短时间旧值。

### 9.2 声明式缓存

```java
@RdtCacheable(region = "products", key = "'product:' + #p0", cacheNull = true, ttl = "10m")
public Product find(long id) { return repository.findProduct(id); }

@RdtCachePut(region = "products", key = "'product:' + #result.id()", ttl = "10m")
public Product save(Product product) { return repository.save(product); }

@RdtCacheEvict(region = "products", key = "'product:' + #p0")
public void delete(long id) { repository.deleteById(id); }
```

- put/evict 默认在方法成功后执行；存在事务时使用提交后回调，避免写出未提交状态。
- `beforeInvocation=true` 的 evict 在业务前执行，回滚不能还原删除。
- 外层实际事务中的 cacheable 绕过缓存；事务与数据库才是事实源。
- 缓存同步失败不等同于数据库回滚；对强一致读取不要依赖 L1 或 Cache Aside。

### 9.3 共享状态独立于缓存

Starter 将以下状态存储装配到 runtime Redis；`CacheService` 使用 cache Redis。状态 API 不经过 L1、不增加随机 TTL，也不发布缓存失效事件：

| 接口 | 用途 | 保留策略 |
| --- | --- | --- |
| `SharedStateStore` | 最新业务快照、异步响应、协调状态 | 每次写入显式指定 TTL |
| `ExpiringMapStore` | 按字段过期的 Map | 每个字段显式 TTL |
| `PersistentMapStore` | 无自动过期的持久 Map | 合并写入，无自动 TTL |
| `CounterStore` | 原子计数 | 无自动 TTL |

```java
state.put("world-state:latest", snapshot, Duration.ofMinutes(5));
Optional<Snapshot> latest = state.get("world-state:latest", Snapshot.class);
```

这些接口表达存储责任，不提高 Redis 自身的持久化保证。事实不能丢失时，需要同时配置 Redis 持久化与备份，或由数据库承担最终事实源。

## 10. 分布式 ID

```java
long id = ids.next("orders");
long serial = ids.next("invoice-sequence", IdMode.ATOMIC);
long migrated = ids.nextAtLeast("legacy-users", existingMaximum + 1);
String display = ids.nextFormatted("invoices", "INV-", "yyyyMMdd-", 8);
```

- 默认 SEGMENT，起点 1，每次申请 5000 个号；减少 Redis 往返，但实例退出会留下空洞。
- ATOMIC 每次按整数 CAS 递增，可给包含式下界；不使用 Lua 浮点数计算大整数 ID。
- 两种模式是不同序列空间，同一业务不能无计划切换。
- 格式化日期只是展示，不会自动按日期重置序列；默认时区 UTC。
- Redis 数据丢失/回滚可能破坏唯一性；不要靠删除 key 重置业务主键。

保留的 `@RdtId` 仅适合明确的方法分配边界：方法返回 long/Long，切面使用分配值替换方法结果；`minimum` 只支持 ATOMIC。它不是 JPA 字段生成器，不能与缓存返回值注解组合。

## 11. 限流与租约并发

```java
boolean allowed = rateLimiter.tryAcquire("api:user:" + userId, 10,
        Duration.ofSeconds(1), Duration.ZERO);

try (LockHandle permit = rateLimiter.tryConcurrency("render", 2,
        Duration.ofMillis(100), Duration.ofSeconds(30)).orElseThrow()) {
    renderer.render();
}
```

- QPS 的 permits 是整个 interval 的共享容量，不是本次请求消耗数量。
- 同一个 key 的容量与周期必须一致；变更需要明确运行期策略。
- tryConcurrency 使用可过期许可，崩溃后等租约回收；不续租，任务超过租期可能突破实际并发上限。
- 4GB/数据库池 5 的应用先按下游预算定容量，不因虚拟线程便宜就无限开并发。

同步方法可使用 `@RdtRateLimit(key = "'report:' + #p0", permits = 5, interval = "1s")`。CONCURRENCY 模式选择 `mode = RateMode.CONCURRENCY` 并显式配置 leaseTime。表达式必须由代码控制，不执行用户提供的 SpEL。

## 12. Redis 幂等占位与事件关联

### 12.1 非数据库幂等模板

```java
var acquisition = idempotency.tryStart("request:" + requestId,
        Duration.ofSeconds(30), Duration.ofHours(24));
if (acquisition.isEmpty()) return;
try (IdempotencyHandle handle = acquisition.get()) {
    applyExternalOperation();
    if (!handle.complete()) throw new IllegalStateException("idempotency ownership lost");
}
```

- 成功后 complete；未完成而关闭时 abort，允许重试。
- 业务执行时间应小于占位租约；完成 TTL 应覆盖重复到达窗口。
- 空 Optional 可能表示正在执行或已经完成；需要时用 isCompleted 辅助判断。
- 完成标记与业务库不是同事务；需要 DB 原子消费使用 Inbox。

### 12.2 EventCoalescer

```java
String correlationId = coalescer.observe(fingerprint, source, eventId,
        new EventCoalescer.Policy(Duration.ofSeconds(3), 8, 4));
```

用于多来源上报同一逻辑事件的短窗口关联。fingerprint 是业务内容指纹，source 是来源，eventId 是来源内事件 ID；返回值是本轮关联身份。它保留同来源的重复业务轮次，不是“同内容窗口内一律丢弃”的通用去重器。窗口最多 1 分钟，轮次/来源数量有上限。

## 13. 原子数据结构

注入 `io.github.rdt.redisson.atomic.RedisUtil`：

- `addLong`、`compareAndSet`：整数计数与 CAS。
- `addDouble`：浮点累加，不用于精确金额结算。
- `setBit`、`bitCount`：位标记和数量统计。
- `addDistinct`、`distinctCount`：HyperLogLog 近似去重计数，不是精确成员集合。
- `initializeBloom`、`bloomAdd`、`bloomMightContain`：显式初始化布隆过滤器；有误判，不允许据此作最终权限/唯一性判断。

工具不隐式加业务事务；资源数量、键生命周期和删除由应用设计。

## 14. 容量、准入与指标

### 14.1 主要默认预算

| 配置 | 默认 | 含义 |
| --- | --- | --- |
| `rdt.queue.max-concurrent-handlers` | 16 | JVM 实际处理上限 |
| `rdt.queue.local-buffer-capacity` | 32 | 可靠消息本地等待缓冲；不是无界队列 |
| `rdt.queue.max-blocking-reads` | 4 | 同时阻塞 Redis 读取上限 |
| `rdt.queue.read-batch-size` | 8 | 单次读取上限，读取前预留槽 |
| `rdt.queue.poll-timeout` | 2s | 默认长轮询 |
| `rdt.queue.concurrency` | 1 | 未覆盖监听器并发 |
| `rdt.queue.max-attempts` | 3 | 已确认失败预算 |
| `rdt.queue.visibility-timeout` | 30s | Pending 崩溃恢复阈值 |
| `rdt.queue.stream-retention` / `stream-max-length` | 7d / 10000 | Stream 保留上限，可能淘汰离线组仍需要的数据 |
| `rdt.queue.dead-letter-ttl` | 30d | Redis 死信保留时间 |
| `rdt.schedule.max-concurrent-tasks` | 16 | JVM 调度业务槽位 |
| `rdt.schedule.poll-interval` / `max-due-per-poll` | 1s / 128 | 调度扫描预算 |
| `rdt.schedule.worker-lease` | 30s | 派发/执行租约，至少覆盖三次 poll |
| `rdt.schedule.result-ttl` | 7d | 调度历史与结果保留 |
| `rdt.lock.wait-time` / `lease-time` | 3s / 0s | 默认等待/watchdog |
| `rdt.instance.lease` / `renew-interval` | 30s / 5s | 稳定身份租约/续期间隔 |

默认可靠 in-flight 受 16 处理 + 32 缓冲预算约束；listener 自身并发继续叠加约束。节点、job、listener 是不同层次，不能用一个数字替代全部容量。

### 14.2 压力准入

```yaml
rdt:
  admission:
    enabled: true
    cpu-pause: 0.85
    cpu-resume: 0.65
    database-pause: 0.90
    database-resume: 0.70
    sample-interval: 2s
    recovery-delay: 5s
```

- CPU/数据库压力过高暂停新工作，恢复阈值和稳定期避免抖动；已开始任务继续续租。
- 数据库压力来自应用提供的 DatabasePressureProbe；不因基础 Starter 引入 JDBC。
- draining 是操作性排空；pressure 是自动背压，两者都影响新工作但不是身份租约丢失。

### 14.3 指标

存在 Micrometer MeterRegistry 时自动接入；否则使用 no-op 指标实现，不改变业务行为。

| 指标 | 类型 | 标签 |
| --- | --- | --- |
| `rdt.schedule.dispatch` | counter | result=ok/no_worker/transition_lost/submit_failed |
| `rdt.schedule.capacity.rejected` | counter | job=taskId |
| `rdt.schedule.dispatch.rollback` | counter | reason=capacity/draining/unregistered/submit_failed |
| `rdt.schedule.lease.lost` | counter | job=taskId |
| `rdt.schedule.reservation` | gauge | node=instanceId |
| `rdt.queue.reclaim` | counter | result=claimed/skipped_not_due |
| `rdt.queue.dlq.saved` | counter | destination、mode |
| `rdt.queue.broadcast.failure` | counter | destination |
| `rdt.queue.broadcast.dropped` | counter | destination |

- 不给 messageId/executionId 打 tag，避免时序基数失控。
- capacity 拒绝计数与成功 CAS 的 capacity rollback 成对增加；旧 token 重复送达不重复计数。
- reservation 是 Redis 租约账，local active 是实际执行；派发途中、GC、网络故障和 lease 过期前可暂不相等，不能保证永远 ±1。
- 启动日志打印脱敏配置摘要、指纹、每个 Redis 客户端 host:port/db 及默认锁租约；不输出密码和完整 Redisson Config。

## 15. 独立 Console、死信重放与归档

### 15.1 管理端

默认 HTTP 端口 `9898`，API 前缀 `/rdt/management`。Console 无需 Redis 或业务数据库即可启动和登录。首次启动自动创建最高权限 ADMIN 用户 `rdt / rdt`，账号、角色和会话保存在本地；Redis 目标登录后配置。不需要注册或业务数据库。重启不会重置修改过的密码。

- 登录：`POST /auth/login`；请求 JSON `{ "username": "...", "password": "..." }`。
- 命名空间、总览：`GET /namespaces`、`GET /overview`。
- 实例、Stream、任务、死信：`GET /instances`、`/streams`、`/tasks`、`/dead-letters`。
- 任务历史：`GET /tasks/{taskId}/attempts`、`/executions`。
- 控制：`POST /tasks/{taskId}/trigger`、`/paused`、`/executions/{executionId}/retry`。
- 排空：`POST /instances/{instanceId}/draining`。
- 危险删除/死信重放：使用现有页面确认操作，不把物理 Redis key 暴露给调用方。
- 页面使用共享 Redis 状态，不直连业务 JVM 内存；命令不能强行中断已执行到外部系统的回调。

列表使用有界分页；namespace 在登录后的“连接设置”中限定，留空则从 runtime Redis 自动发现。管理页保留真实运行数据、分页和危险操作确认，页面和 API 均按当前账号权限开放。

### 15.2 单 JAR 托管 Koi UI 与 API

Console 的运行形态是 **一个 Java 进程、一个 JAR、一个 HTTP 端口**。默认打开 `http://127.0.0.1:9898`，页面、JS/CSS/字体和 `/rdt/management` API 均由内嵌 Tomcat 同源提供；没有第二个前端服务、外站跳转或分开部署步骤。

页面使用桌面 Koi UI 原工程的完整布局：纵向、经典、横向、分栏、综合五种布局，以及移动端抽屉。登录页使用原工程的插画、光斑和浮动动效；右上角保留可折叠工具栏、菜单搜索、刷新、尺寸、语言、明暗切换、浏览器全屏、主题面板和用户菜单。主题面板可切布局、主题色、菜单反色、页签样式和路由动画。首页采用上游首页的欢迎区与卡片结构，数据来自真实 RDT API；Logo/图标使用自有 RDT 标记。语言切换覆盖上游工具栏与组件文案，RDT 业务页面当前以中文为主。

```bash
# 仓库根目录构建：构建机需要 JDK 21、Node >=20.19 和 pnpm。
# package 自动执行锁定依赖安装、类型检查和 Vite 构建，然后生成可执行 JAR。
sh mvnw -pl rdt-console -am clean package

# 运行机仅需 JDK 21；Redis 可在进入软件后再配置。
sh scripts/console.sh
# 浏览器打开 http://127.0.0.1:9898
# 首次登录：用户名 rdt，密码 rdt。已有账号文件不会在重启时被覆盖。
```

独立分发时，复制 `rdt-console/target/rdt-console-1.0.0-SNAPSHOT.jar` 和同目录的 `jvm.options`，运行 `java @jvm.options -jar rdt-console-1.0.0-SNAPSHOT.jar`。也能直接 `java -jar`，但 Java 不会自动读取旁边的 `jvm.options`。

构建产物先生成到 `target/generated-resources/console/static`，再同步到 `target/classes/static` 打包。前者已注册为 Maven 资源根，IDE 重建 classes 后会复制回来；不会再只留下 JS/CSS 却丢首页。Vite 每轮清理生成目录，Maven 不再读取历史 `web/dist`。安装/类型检查/构建失败会让 package 失败，不会悄悄打入旧页面。`/login`、`/tasks` 等深链接由服务端内部转发到包内 `index.html`，浏览器刷新仍留在同一端口。修改源码后重新构建并重启 JAR，旧运行进程不会自动变新。

前端定向检查：在 `rdt-console/web` 执行 `pnpm exec vue-tsc --noEmit`、`pnpm lint`。`mvn test` 不执行前端安装或构建。浏览器回归支持直接测试已启动的 JAR：

```bash
cd rdt-console/web
RDT_CONSOLE_USERNAME=your-test-admin RDT_CONSOLE_PASSWORD=your-test-password \
PLAYWRIGHT_BASE_URL=http://127.0.0.1:9898 PLAYWRIGHT_REAL_API=1 pnpm test:ui
```

该命令不会启动 Vite。UI 夹具测试覆盖五种布局、工具栏、页签右键菜单、主题和移动端抽屉；额外的 packaged-console 测试使用真实登录/API，检查所有页面深链接、无跳转、同源资源和管理功能。该测试需要先在连接设置保存专用 runtime Redis，且 Redis 中存在可发现的 namespace。Playwright Chromium 需预先安装，也可用 `PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH` 指定本机 Chrome。上游来源与许可证见 `web/vendor/koi-ui/README.md` 和 `web/KOI-LICENSE`。

### 本地身份、角色与持久化

1. 首启在本地账号库创建 ADMIN `rdt / rdt`。只在账号文件不存在时执行一次；正常启动读取已有账号。登录后管理员可管理用户和角色，各账号可修改密码；创建用户与修改密码继续要求 12–128 字符。
2. 密码 12–128 字符，用独立随机盐的 PBKDF2-HMAC-SHA256（600,000 次）存哈希。`security.json` 保存账号与会话摘要；不保存明文密码或原始 Bearer Token。不依赖被管理 Redis、JPA 或企业数据库。
3. 登录成功发放随机不透明 Token，浏览器只保存 Token，权限每次从后端会话和账号读取。退出登录、禁用、删除、改角色、重置或自改密码都会在服务端撤销会话；会话重启仍有效，过期后拒绝。
4. 所有管理 API 默认拒绝，按接口校验权限；隐藏按钮仅改善交互。保留至少一个启用管理员。当前固定角色，不开放自定义角色/权限编辑：

| 角色 | 读取运行数据 | 触发/暂停/重试任务、排空实例 | 删除消息/记录、死信重放 | Redis 连接与用户管理 |
| --- | --- | --- | --- | --- |
| ADMIN 管理员 | 是 | 是 | 是 | 是 |
| OPERATOR 运维 | 是 | 是 | 否 | 否 |
| VIEWER 只读 | 是 | 否 | 否 | 否 |

5. 当前最多 100 个本地用户、128 个有效会话；PBKDF2 登录并发有界，同来源同账号连续失败达到阈值返回 429。用户目录只供一个 Console 实例使用，不作为多节点共享身份服务。
6. 本地文件以同目录临时文件写入、刷盘和原子替换，POSIX 下目录 700、文件 600，拒绝直接符号链接；写入失败抛配置异常，不把保存失败伪装成成功。备份整个私有目录，并保护连接密钥、账号文件。

### 登录后配置 Redis 与 runtime/cache 边界

Console 先独立启动，不要求 Redis 存在或可达。首次登录会进入 `/settings`：

1. 在 **runtime：分布式组件** 中选择单机、Cluster 或 Sentinel，填写节点地址（每行一个）、Database、ACL 用户名和密码。Sentinel 另填 master name 与 Sentinel 凭据，TLS 使用 `rediss://host:port`。
2. 点击“测试 runtime 连接”只做连通性检查，不保存、不写业务数据；“保存并应用”才持久化并切换后续管理操作。可以先保存暂时不可达的目标，之后仍可进入设置修正，不让 Redis 故障卡住软件启动。
3. **cache：可选缓存** 独立配置，绝不自动替代 runtime。当前缓存配置仅支持保存和连接测试，尚无缓存数据管理页；不会因填了 cache 就预建一个常驻连接池。
4. 消息 Streams、任务、实例、死信、告警和归档全部使用 runtime。NnmBot 目前的 `rdtRuntimeRedis` / `rdtCacheRedis` 是不同客户端；Console 应填写对应服务的实际地址/DB，不是填写 Java Bean 名，也不会替应用修改 Bean 装配。Sa-Token session Redis 不属于这两种管理角色。
5. 可填写 namespace 白名单，留空才自动发现 runtime 中的 RDT namespace。切换后清空旧 namespace 选择和告警快照。在途查询/命令/归档持有旧连接到完成，之后释放；新请求使用新连接。
6. 所有管理写命令携带 `X-Rdt-Connection-Revision`；连接已经切换或进程重启时，旧页面命令返回 409，必须重新读取设置再发，避免对另一个环境误操作。

连接配置默认保存到 `~/.rdt-console`，可用 `RDT_CONSOLE_CONNECTIONS_DIRECTORY` 指定绝对目录。`connections.enc` 使用 AES-GCM，加密密钥为同目录私有 `connections.key`；目录/文件在 POSIX 下权限 700/600。保存采用同目录临时文件、force 落盘与原子替换，失败时不切换生效配置。备份需同时保留并保护这两个文件；拥有该用户文件访问权限的人也能拿到密钥，不把本地加密当成外部密钥托管。

接口不回显密码；表单留空保留已保存密码，勾选清除才删除。浏览器只存登录 Token 与 namespace，不保存 Redis 密码。连接设置接口同样要求 Console 本地登录，Redis 登录失败不影响本地账号认证。原 `spring.data.redis.*` / `spring.redis.redisson.config` 不再作为 Console 的连接入口，没有另一条启动配置兼容分支。

| 配置 | 是否适合运行后设置 | 当前实现 |
| --- | --- | --- |
| Redis 地址、拓扑、DB、ACL/TLS、runtime/cache 角色 | 是 | 已在连接设置即时应用 |
| namespace 管理范围 | 是 | 已在连接设置即时应用 |
| 告警阈值、采样周期 | 适合独立做运行时策略，但需真正重调度监控器 | 本轮仍使用 YAML，修改后重启 |
| 归档开关、目录、cron、时区、保留期 | 适合做受控管理配置，必须处理在途归档与调度重建 | 本轮仍使用 YAML，修改后重启；默认目录 `~/.rdt-console/archive` |
| HTTP 端口、API 路径、虚拟线程、连接预算、JVM 参数 | 保留启动配置 | YAML / JVM options |
| 本地账号、密码、角色、启用状态 | 运行后配置 | 管理员在“用户与权限”维护，改角色/密码/禁用立即撤销该用户会话 |
| 会话有效期、私有配置目录 | 保留启动配置 | YAML / 环境变量，修改后重启；不配置 Token 签名密钥 |

IDE 启动前先执行一次 package 生成前端，随后 IDE 重编译可从生成资源根恢复首页。`clean` 会删除 target 下的生成资源，需要重新 package；缺首页时启动会给出明确构建命令，不再静默启动后返回 404。运行已打包 JAR 则没有该目录依赖。

### 15.3 死信重放

- 默认 Redis 存储；STREAM 与可靠广播的 field 为 `effectiveGroup:messageId`，在线广播再追加 instanceId。
- 重放保留模式和业务载荷，生成新 ID，携带 originalMessageId；每目的地限速 10 次/秒。
- 先发送，后删死信。两步不是跨 key 原子事务，崩溃可造成重复；不允许先删后发制造静默丢失。
- 可靠广播重放会被原 Stream 上各消费组看到；在线广播重放面向当前全部在线订阅者，不是精确单实例补发。
- 删除 Stream 消息会 ACK 相关 Pending 并删除原文，不能撤销已经开始的业务回调。
- 有序死信重放是新事件，不恢复原先历史位置；需要业务层评估顺序影响。

### 15.4 告警与异常归档

- 告警查询 `/alerts/dead-letters`，根据死信总量、近期增长和最老年龄分级，并按时间抑制重复告警。
- 默认异常归档每日 02:00（Asia/Shanghai），输出 GZIP UTF-8 JSONL；目录、cron、保留天数均可配置。
- 本地开发把 `rdt.console.archive.directory` 设置为可写绝对路径，例如 `/tmp/rdt-archive`，或关闭 archive。
- 每轮有界批次读取共享 Redis，覆盖死信和异常调度；归档不会删除业务 Redis 记录或延长其 TTL。
- 本地输出使用同目录临时文件、完成后发布；失败中止，不发布半成品；最终文件成功后才执行本地过期文件清理。
- SCAN/HSCAN 不是一致性快照，可能重复/遗漏；归档不是审计事件流或 exactly-once 数仓。
- RdtArchiveSink/Factory 可扩展存储；未知 sink 应明确失败，不回退本地吞错。

### 15.5 内存与线程预算

- Console 启动时不创建 Redis 客户端；保存 runtime 后首次查询才创建一个小型原生 `RedissonClient`。cache 是独立可选配置，当前仅保存和测试连通性，不创建闲置常驻客户端。已移除 Spring Data Redis、RedisTemplate 和连接工厂装配，不引入业务 starter、队列消费者或业务 worker。
- 每个实际使用的客户端默认 Redisson 普通/Netty 线程各 2 个；单机命令连接 min=1、max=4，订阅连接 min=max=1。界面支持单机、Cluster、Sentinel 和 rediss TLS；数据库与凭据由表单设置，Cluster 固定 DB0。连接测试一次只允许一个，结束立即关闭临时客户端。
- 归档只允许单线程、零排队，空闲 30 秒回收 worker；后续任务重新创建。归档本身的周期、去重、关停等待和不积压语义保持不变。
- HTTP 保留虚拟线程、最多 128 个连接、32 个待接入连接和 15 秒 keep-alive；虚拟线程不代表可以无限拉取 Redis 数据。扫描仍分批，推荐在界面显式限制 namespace。
- `scripts/console.sh`、分发的 `jvm.options` 和 `spring-boot:run` 默认使用 `-Xms32m -Xmx256m -XX:+UseSerialGC`，适合小型低流量管理端。**没有把堆上限硬砍到 64/128 MB**，也不限制 metaspace/direct memory 来掩盖占用。
- 若管理端变成高并发工作负载，应先压测停顿/吞吐，再在自己的 options 文件中换用 G1；也可直接执行 `java @jvm.options -XX:-UseSerialGC -XX:+UseG1GC -jar rdt-console-1.0.0-SNAPSHOT.jar`。普通 `java -jar` 不会自动套用 options 文件。

上一轮固定连接版本在 2026-10-19 的本机 JDK 21 对照实测（本轮按需连接版本没有将此作为重新压测结果）：同一个专用 Redis、归档开启、350 次管理 API 请求（并发 4），诊断性显式 GC 后等待 5 秒，再取 5 次 RSS 中位数。修改前默认 G1 为 **231.0 MiB**，精简依赖并使用默认启动脚本后为 **204.2 MiB**，约下降 **11.6%**；JVM 线程样本 46 → 33，JAR 45.4 → 40.9 MiB。该数据只说明小数据集下的对照结果，不是生产压测或内存峰值承诺。测量时两边都开启 NMT，正常运行默认不开 NMT、不强制 GC。

`-Xmx256m` 只限制 Java 堆，**不是进程总内存**；还需要为 metaspace、JIT、线程栈、Netty/native 内存和归档批次留预算。大载荷、namespace 数量和并发访问会改变结果，SCAN COUNT 也不是严格的条数/字节上限。Vue 在浏览器执行；JAR 只提供静态文件，不在服务端再启动一个 Vue/Node 运行时。

## 16. NnmBot 接入现状

- producer 使用 RdtMessageSendManager.send 显式 DIRECT，按现有场景选择 STREAM 或 RELIABLE_BROADCAST。
- 当前消费者均明确 `inbox = DISABLED`：缓存、世界状态刷新、拓扑和发现通知不以本次消费写业务库，不额外占用数据库连接。
- 未添加一致性模块依赖、未启用 rdt.consistency；application.yml 仅注释预留。
- runtime/cache/session 客户端按用途声明；逻辑分库不承诺物理资源隔离。
- NnmBot 的本地缓存继续由 cache2k 提供；后端业务权限边界仍为 Sa-Token。

## 17. 开发验证

常规开发验证不执行生产打包，也不需要启动已移除的示例应用；构建单体 Console JAR 使用第 15.2 节的明确命令。

```bash
# 无 Redis 单元测试、装配契约与 H2/JPA 事务测试
sh mvnw test

# 已运行的专用本地 Redis；避免使用真实业务库
# 显式调用 Failsafe，跳过 package 生命周期
sh mvnw test failsafe:integration-test failsafe:verify \
  -Drdt.it.redis=redis://127.0.0.1:6379
```

- IT 使用独立 UUID namespace，不执行 FLUSHDB/FLUSHALL，不需要修改业务键。
- Cluster IT 通过独立的 `rdt.it.cluster` 属性启用，格式以测试类注解/配置为准；未提供时明确跳过。
- 可选资源基准测试另有开关，不把跳过计作已验证。
- 验证覆盖业务回滚、租约回收/旧 token 围栏、并发 Inbox 去重、JPA 同事务、真实 Redis 中继、可靠广播组、队列恢复、广播退避、调度拒绝和默认锁等待契约。
- 本轮方言逻辑有 H2 自动化测试；不能以 H2 代替真实 PostgreSQL/MySQL 的发布前验证。

### 本地 SDK 与独立业务仓库联调

NnmBot 与 RDT 是独立仓库。RDT 源码测试通过不意味着本机 Maven 缓存里的同版本 SNAPSHOT 已刷新；若业务项目仍报找不到新 API，不应退回旧接口或添加兼容别名。

先在 RDT 执行前述测试，再仅刷新本地开发库（不执行 package 生命周期、不构建 Console/NnmBot 生产应用包）：

```bash
sh mvnw -N install:install
sh mvnw -pl rdt-core,rdt-spring-boot-starter,rdt-consistency-jdbc-starter \
  jar:jar install:install
```

然后在 NnmBot 执行 `sh mvnw -pl nnm-app -am test`。安装可选 JDBC SDK 到本机缓存，不等于业务项目增加该依赖或启用一致性模块。

Console 前端仅检查类型可在 `rdt-console/web` 执行 `pnpm exec vue-tsc --noEmit`，无需触发生产构建。

### 开发期与上线边界

1. 本仓库当前选择直接更新状态/键/API，不混入历史双读、双写或迁移开关。
2. **这不意味着当前键格式变更可直接滚动上线。** 有旧数据/旧节点时先安排同版本切换、受控清理或单独设计迁移。
3. 正式滚动升级的版本键、双读写和回滚策略必须另行实现、测试，不能在 README 中假称已经支持。
4. Redis 持久化、备份、noeviction、ACL/TLS、热点预算和故障转移测试仍是运维前提，不属于库提供的 exactly-once 保证。

## 18. 异常体系与暂缓事项

RDT 自身异常统一继承抽象 `RdtException`，并保留稳定 `code()` 和原始 cause。具体类型包括 `RdtConfigurationException`、`RdtRedisException`、`RdtLockException`、`RdtQueueException`、`RdtScheduleException`、`RdtConsistencyException`、`RdtSerializationException`、`RdtRateLimitException`、`RdtExpressionException`。Console 归档使用同一基类下的 `RdtArchiveException`。业务回调原始异常不统一包装为 RDT 异常；JDBC/事务基础设施错误只在 JDBC 边界集中转换为 `RdtConsistencyException`。

异常类型用于识别组件故障，`RetryClassifier` 用于决定业务回调是否值得重试，两者不混为一个继承树。线程中断、资源释放、后台轮询保活和 AFTER_COMMIT 语义需要的 catch/finally 保留；普通调用链不做重复记录或无意义换类型。

按当前要求暂缓的功能、生产专项验证和滚动升级设计统一登记在 [docs/待办.md](docs/待办.md)，本轮不扩展这些能力。
