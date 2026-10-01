# 事务性消息与本地消息表

Pair-wise GSB 标注任务仓库（第 15 批 / 230）。

| 项目 | 内容 |
|------|------|
| 任务类型 | Feature 迭代 |
| 任务难度 | 困难 |
| 语言/框架 | Java, Maven, JUnit 5 |
| 环境可复现等级 | 无外部依赖 |
| 构建方式 | Maven（含 mvnw wrapper，无需本机安装 Maven） |

> 本仓库是**初始环境快照**：只有工程骨架，不含任何实现代码。
> 分支说明：`main` 为初始环境；`A`、`B` 为两次独立执行各自的工作分支，均从 `main` 的同一个提交拉出。

## 运行方式

```bash
./mvnw -q verify
```

## 任务提示词

以下为本题完整的 User Prompt 原文，两次执行必须使用完全相同的文本。

我们写库成功后还要发消息，两者不可能同时成功，现在偶尔出现数据写了消息没发。请从零实现一个事务性消息组件（本地消息表方案）。仓库目前只有一个空的 Maven 工程（pom.xml 只声明 JUnit 5 与 AssertJ）。要求：1) 支持把业务数据与待发消息在同一个本地事务中写入（用内存中的两个存储模拟），要么都成功要么都失败；2) 支持后台投递：轮询待发消息并投递给下游，成功后标记已发送；3) 支持失败重试：投递失败按退避重试，超过上限标记为需人工处理并记录原因；4) 支持幂等消费：下游重复收到同一消息（按消息 ID）不得重复生效；5) 支持重启恢复：进程重启后未完成投递的消息要继续投递，已完成的不重复投递；6) 支持消息顺序：同一业务键的消息按产生顺序投递；7) 提供统计：待发数、已发数、重试次数、失败数与各状态耗时；8) 测试覆盖事务原子性、后台投递、失败重试、幂等消费、重启恢复、顺序保证与统计；`mvn -q verify` 一条命令跑通。

## 提交要求

1. 在本仓库中完成提示词要求的全部内容。
2. `./mvnw -q verify` 必须通过。
3. 完成后在所属分支（A 或 B）上提交，产物快照的父提交必须是初始环境快照。

---

## 实现说明（分支 A）

本地消息表方案实现，代码位于 `com.example.gsb.txmsg`：

| 类 | 职责 |
|------|------|
| `LocalDatabase` | 协调两个内存存储（`InMemoryBusinessStore` 业务表 / `InMemoryMessageStore` 本地消息表）；事务暂存 + 原子提交 + 原子文件快照持久化 |
| `MessageEnvelope` / `DeliveryAttempt` / `MessageStatus` | 消息聚合根：状态（PENDING/SENT/DEAD_LETTER）、单调序号、投递历史、退避累计、死信原因 |
| `RetryPolicy` | 指数退避：`base * 2^(n-1)`，超过 `maxAttempts` 标记 DEAD_LETTER |
| `MessageDispatcher` | 单线程后台轮询；每次投递结果立即落盘；同一 businessKey 只投队头（退避中/死信会阻塞后续消息，保证顺序） |
| `IdempotentConsumer` | 按消息 ID 去重的下游包装；去重集合可文件持久化，重启后仍不重复生效 |
| `MessageStatistics` | 待发/已发/死信数、总投递次数、重试次数与各状态耗时（发送时延、待发时长、死信时延、退避等待） |
| `TransactionalMessageService` | 门面：`inTransaction`、`start`、`statistics`、`requeue`、`close` |

关键设计：

1. **事务原子性**：事务内的业务写入与发消息只进入暂存区，action 正常返回后才同时写入两个存储并落盘；抛异常则两者均不可见。
2. **至少一次 + 幂等**：投递成功标记 SENT 后崩溃不会重投；若下游已生效但标记前崩溃，重投由 `IdempotentConsumer` 按 ID 去重，效果恰好一次。
3. **重启恢复**：提交、标记 SENT、重试、死信都即时写入 `local-database.bin`（临时文件 + 原子 rename）；重新打开即恢复。
4. **顺序保证**：每条消息有全局单调序号，同一 businessKey 严格按序，队头未完成（含退避/死信）时后续不投递。
5. **人工处理**：死信消息不再投递并记录原因；处理后可调用 `requeue(messageId)` 重新入队。

测试（22 个，`src/test/java/com/example/gsb/txmsg`）：`TransactionAtomicityTest`、`BackgroundDeliveryTest`、`RetryAndDeadLetterTest`、`IdempotencyTest`、`RestartRecoveryTest`、`OrderingTest`、`StatisticsTest`。

```bash
mvn -q verify   # 或 ./mvnw -q verify
```
