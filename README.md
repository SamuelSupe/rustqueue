# RustQueue 0.6（生产候选）

RustQueue 是一个 Rust 实现的、兼容 NSQ V2 数据面的持久化消息队列。0.6 使用磁盘优先的 format v6，将每个 topic partition 放入独立 OpenRaft 组，并以 bounded Home Cell 组成可横向扩展的 federation。每个 Home Cell 内使用 RF=3/RF=5 多数派持久化确认；Root、Catalog、Cell metadata 和 partition 使用互不冲突的 `GroupKey`，不会把全局节点放进同一个 Raft membership。

当前代码已闭合 0.6 的功能性 P0：独立 Root/Catalog Raft、透明跨 Cell 发布/消费/FIN/REQ/TOUCH、Catalog channel 生命周期、可恢复 partition migration、稳定 command envelope 与在线滚动门禁均进入实现和验收路径。仓库仍称为“生产候选”，因为 24 小时 soak、目标生产卷故障演练和正式性能报告必须在实际部署环境完成，不能由短时本机测试替代。

## 0.6 federation 边界

已实现并进入持久化/验收路径：

- 全局节点数量不再有 9 节点配置上限；partition 与 Cell 元数据只使用本 Cell 的 3–9 个节点，避免全局节点数进入数据面复杂度。
- format v6 持久化 `CellId + local group ID + log index + batch ordinal + incarnation` 的内部消息身份，同时继续输出 NSQ 兼容 16 字节十六进制 wire ID。wire slot 是 topic-local 身份，不再消耗集群级每 partition 生成器；复用前必须证明 segment、snapshot、ACK 和 in-flight 引用全部归零并经过 quarantine。
- Catalog 持久化 Home Cell、65,536 个弹性 routing bucket、topology generation、routing epoch、最多 128 个 Home Cell/topic（可配置），普通发布跳过不可用 Cell；key/显式 partition 在 Home Cell 不可用时 fail closed。
- Root 独立于 Cell metadata 运行，Catalog shard 使用独立 Raft group；现有数据流量使用有界缓存，不同步依赖 Root。partition 跨 Cell migration、资源感知 balance 和 scoped feature level 都有可恢复状态机与不变量测试。
- libp2p 发现使用 Cell 内连接和 router-to-router 跨 Cell 连接；Gossipsub 公告带身份绑定、认证和 TTL，Kademlia 保存路由地址。lookup 在 federation 模式下每个就绪 Home Cell/topic 最多返回一个 gateway。
- 任意 gateway 可按 Catalog route 转发 publish、fetch、FIN、REQ 和 TOUCH；结构化 stale-route 最多有界刷新重试，不做写 hedge。持久 channel 和 ephemeral lease 由 Catalog 协调跨 Home Cell 生命周期。
- partition migration 实际执行 target prepare、learner copy/catch-up、短 source fence、joint consensus、目标 voter 选举、原子 Catalog cutover 和源副本 retire；每个边界可幂等恢复。
- 提供 `/v1/federation`、`/v1/federation/catalog`、`/v1/federation/route`、`/v1/federation/stats`、`/v1/federation/operations` 以及带 epoch 校验的 `/v1/pub`、`/v1/mpub` 原生接口。

冻结边界：

- 0.6 默认运行一个独立 Catalog shard；range-map 与 split 状态已稳定，但自动 Catalog split executor 仍关闭，不作为本版本能力声明。
- 不提供旧数据格式迁移、exactly-once、跨地域复制、在线备份、热点感知迁移或管理前端。
- 默认最多 128 个 Home Cell/topic，可配置提高；单个 Cell 保持 3–9 个节点。扩展全局容量应增加 Cell，而不是扩大单个 partition membership。
- 本机 3 Cell/9 节点验收覆盖 150 条跨 Cell ledger、在线 migration、TOUCH/REQ、ephemeral 删除且 `missing=0`；5 节点 rolling gate 在 121 秒内完成 9 次 graceful restart，对账 4,321 条已确认消息且 `missing=0`。这些结果不是长期生产 soak 的替代品。

## 核心能力

- 单 Cell 3–9 个节点；全局节点清单不设产品级硬上限。partition 只允许 RF=3 或 RF=5，每个 Cell 的 metadata voter 只允许 3 或 5 个。
- libp2p 自动发现使用持久 PeerId、Noise、Yamux、Identify、Ping、seed peer exchange 和可选 mDNS；发现节点只有经元数据 Raft 准入后才能成为 learner。
- 内部使用 `root`、`catalog-<shard>`、`cell-<id>-meta`、`partition-<origin-cell>-<local-id>` GroupKey；legacy `group_id=0` 仅在管理 API 中表示本 Cell metadata。每个 partition 有永久全局身份、topic-local wire slot 和独立 Raft 组。
- 4 节点 RF=3 会轮换三节点组合；5 节点默认 RF=3，关键 topic 可选择 RF=5。
- partition 统一 segment 只写一次 Raft entry 和消息 body；内存状态只保存固定大小的消息元数据与磁盘引用。
- 节点级 64 MiB 有界 payload cache、受限异步读取 worker/队列，以及逻辑 Raft purge 与物理 segment GC 分离。投递采用锁内预留、锁外读取、锁内提交；慢盘和 cache miss 不再占用 partition mutex，队列满时直接向消费者施加背压。
- 快照先封存当前 segment，再以 hard link 复用不可变 segment；partition 投影使用独立二进制文件，每 4096 条消息短暂取锁后流式编码，状态文件不再嵌入 O(backlog) 的消息 Vec。跨节点传输使用懒打开 segment 的可 seek 虚拟归档流，接收端逐文件校验 CRC，并以临时 generation、文件与目录 fsync 和原子 `CURRENT` 切换完成安装。
- channel 创建/删除屏障、每 partition ACK cursor、最多 65,536 条 sparse ACK、至少一次投递和 Leader 故障后的安全重复投递。
- 单条消息可配置到 32 MiB，MPUB/HTTP 请求最多 64 MiB；节点级与连接级字节 admission 在分配正文前执行，超过内存预算或磁盘水位时 HTTP 返回带 `Retry-After` 的 429。小请求继续使用 1 ms/64 条、最多 8 MiB 的 group commit，大请求作为单独 Raft entry 提交。
- 每 partition 硬 backlog quota、磁盘高水位写门禁、可配置消息保留期和最大投递次数。过期或 poison message 先多数派发布到每 channel 独立 DLQ topic，再提交源 FIN；故障窗口最多造成 DLQ 重复，不会先删源消息。保留期为 `0` 时关闭按年龄淘汰。
- 保护性淘汰默认先给节流、普通 ACK GC 和副本迁移 60 秒恢复窗口；只有全部可用节点都没有合格存储目标、没有进行中的 membership 操作且所有节点已激活对应 feature level 时，partition Leader 才会按最老完整 segment 提交 quorum eviction。随后强制生成快照并 purge，操作写审计日志和 Prometheus 计数器；不会本地单副本静默删消息。
- 多 gateway ephemeral channel 使用复制租约；首个 lease 与创建准备命令在同一个 Raft entry 中提交，最后一个消费者离开后幂等删除。
- topic 可不停机增加 partition；新 topology 一次性激活，不迁移历史消息，已有 TCP 连接无需重连。
- 自动失联副本替换、Leader/副本均衡、元数据 voter 替换；membership 操作将 `transfer_leader → add_learner → catch_up → joint_consensus → remove_old → retire` 的下一动作逐阶段持久化并按实际 Raft membership 幂等重入。类型化瞬时错误自动续跑，策略/资源错误停在 `needs_operator` 并等待显式 resume。
- 显式 rebalance、整节点 drain、maintenance TTL、Leader 转移、learner 追平、joint consensus、在线副本 quarantine/重建和 scrub；整节点 drain 固化 group 清单、当前 group、目标 voters 和 membership phase，Leader 故障后从已提交游标继续。
- 500 ms 时钟漂移保护、健康 gateway lookup、quorum/存储/时钟联合 readiness、结构化日志，以及 fsync、group commit、转发、snapshot build/install、GC、repair 的 Prometheus 固定桶延迟直方图。
- Raft segment、vote/applied boundary、状态机 apply、快照 generation 和 GC 的同步文件操作统一进入进程级有界阻塞执行器，不占用 Tokio worker。高流量内部 Raft、发布、投递和确认转发继续复用 mTLS HTTP 连接，但载荷使用带 magic、版本、长度和 CRC32C 的二进制帧，不再通过 JSON 展开消息正文。
- 消费投递使用 `FetchBatch(max_messages, max_bytes, wait_ms)`：每次最多 64 条/1 MiB，默认长轮询 100 ms，并由 partition `Notify` 提前唤醒。topic/channel 的消费者共享 claim/ready 队列，每个外部 fetch 最多发出两个互不重复的 readiness probe；非空批次只执行一次同 term 合并的线性化 quorum 确认。
- Gateway 缓存 `(group_id, leader_id, term, topology_epoch)`，Follower 返回结构化 `NotLeader` 并由 Gateway 串行重定向，Follower 不再二次代理，写请求也不做可能造成重复发布的并行 hedge。
- 内部 mTLS RPC 启用 HTTP/2 多路复用和 keepalive；snapshot 使用独立连接池。vote/control、write、fetch response、AppendEntries、snapshot 分别使用 64 KiB、80 MiB、40 MiB、80 MiB、8 MiB 硬边界。
- Follower catch-up 一次最多请求 64 个 entry；网络层按编码后的 16 MiB 硬边界切分并返回 OpenRaft `PartialSuccess`，小消息可宽批追平，大 MPUB 不会突破端点上限。
- FIN/REQ 进入节点级有界 ACK pipeline，按 partition 在 1 ms/64 条内批量复制；单连接不再等待每条 ACK 的 Raft commit。
- ACTIVE 持久 channel 的 SUB 使用已提交元数据快速路径，不重复写 CreateChannel；多个消费者的初始 partition 游标使用低差异序列分散，避免一起扫描相邻 partition。
- `/v1/health` 对 ACTIVE group 使用最多 32 路有界并发 quorum 检查，1024 partition 不再被串行 readiness 扫描拖慢。

## NSQ 兼容

TCP 支持 `IDENTIFY`、`AUTH`、`SUB`、`PUB`、`MPUB`、`DPUB`、`RDY`、`FIN`、`REQ`、`TOUCH`、`NOP`、`CLS`。IDENTIFY 协商结果会真实作用于连接：heartbeat、消息 timeout、输出 buffer 大小/flush timeout、`sample_rate`、TLS、Snappy 和 Deflate 都不是仅回显字段。AUTH 会携带 TLS 与客户端证书 common name，TTL 到期后 fail-closed 刷新授权。

`make compat` 会启动隔离的明文与 TLS+mTLS+AUTH 服务，使用官方 `go-nsq` 和 `pynsq` 验证直连、lookup、Snappy、Deflate、PUB、MPUB、DPUB、RDY、FIN、REQ、TOUCH、fan-out、sampling、ephemeral channel、授权刷新和错误 secret 拒绝。测试证书和数据卷在结束时自动清理；`make compat-core` 可针对已经运行的明文服务执行同一核心矩阵。

HTTP 支持：

- nsqd：`/pub`、`/mpub`、`/stats`、`/ping`、`/info` 和 topic/channel create、delete、empty、pause、unpause。
- lookupd：`/lookup`、`/topics`、`/channels`、`/nodes`、`/ping`、`/info`。

`/lookup` 返回健康且未 drain 的 gateway，而不是 partition Leader。官方消费者可以同时连接多个 gateway，它们仍共享同一 channel 状态。

协议目标以 [NSQ TCP 规范](https://nsq.io/clients/tcp_protocol_spec.html) 和 [nsqd HTTP 接口](https://nsq.io/components/nsqd.html) 为准。首版不兼容 NSQ 磁盘格式、lookupd TCP 注册、tombstone、nsqadmin UI 或 StatsD。

## 快速启动

构建和测试默认在 OrbStack 容器中执行，宿主机不需要安装 Rust：

```sh
make fmt
make clippy
make test
make acceptance-discovery
make up

curl -d 'hello' 'http://127.0.0.1:4151/pub?topic=events'
curl 'http://127.0.0.1:4151/stats?format=json'
```

NSQ 客户端可直连 `127.0.0.1:4150`，也可把 `http://127.0.0.1:4151` 当作 lookupd 地址。

多节点示例：

```sh
make cluster-up       # 3 节点，metadata RF=3，partition RF=3
make cluster4-up      # 4 节点，metadata RF=3，partition RF=3
make cluster5-up      # 5 节点，metadata RF=3，partition 默认 RF=3
make cluster5-rf5-up  # 5 节点，metadata RF=5，partition 可选 RF=5
make cluster9-up      # 9 节点，metadata RF=3，partition RF=3
```

## Kubernetes Operator 部署

`deploy/helm/rustqueue` 提供 CRD、Operator、RBAC 和集群实例。生产默认是一台合格 Kubernetes 节点承载一个 Broker；每个 Broker 使用独立的单副本 StatefulSet 和 RWO SSD PVC，因此 Pod 漂移后仍绑定原来的持久身份。PVC 默认 `Retain`，卸载 Helm release 不会静默删除消息卷。

先为专用节点增加标签、taint 和故障域标签，并确认所选 StorageClass 支持 PVC 重新挂载：

```sh
kubectl label node worker-1 worker-2 worker-3 rustqueue.io/dedicated=true
kubectl taint node worker-1 worker-2 worker-3 \
  rustqueue.io/dedicated=true:NoSchedule

helm upgrade --install rustqueue deploy/helm/rustqueue \
  --namespace rustqueue-system --create-namespace \
  --set operator.image.repository=registry.internal/rustqueue-operator \
  --set operator.image.tag=0.6.0 \
  --set cluster.image=registry.internal/rustqueue:0.6.0 \
  --set cluster.storage.className=ssd-rwo
```

若 `storage.className` 留空，Operator 只在集群恰好有一个默认 StorageClass 时自动选择；生产仍建议显式指定 SSD 类。Broker 的 4150/4151 只由 namespace 内的 ClusterIP Service 暴露，不创建 Ingress、LoadBalancer 或 NodePort。

Operator 会执行以下闭环：

- 生成私有集群 CA、每 Pod 双用途 mTLS 证书、discovery token 和管理 token；Broker 只能挂载 CA 公钥和自身叶证书，CA 私钥只保存在 Operator 可读 Secret 中。叶证书在到期前自动续签并走一次只替换一个 Broker 的滚动流程。
- 按节点标签自动增加 Broker；一个新 Cell 至少凑齐 3 个节点后才激活，避免暴露无 quorum 的半 Cell。每个 Cell 仍限制为 3–9 个 Broker，全局容量通过增加 Cell 扩展。
- 将 Broker 稳定分配到具体节点。cordon、drain 或持续 NotReady 后，有合格替代节点时更新 StatefulSet 目标、保留 PVC 身份并自动迁移；没有安全目标时保持原副本和 `Degraded`，不会降低 RF。若希望预留立即迁移能力，应配置固定 `nodes.replicas` 并准备额外的已标记空闲节点；自动按节点扩容会使用全部合格节点。
- 更新 `spec.image` 后，先设置 Broker maintenance、转移 Leader，再使用 `OnDelete` 一次替换一个 Pod；替代 Pod 通过 readiness 和版本探测后才继续。相同镜像标签需要重拉时，递增 `spec.upgrade.retryGeneration`。生产建议使用不可变 tag 或 digest。
- 全 Kubernetes 集群只允许一个 `RustQueueCluster` 成为活动实例；额外 CR 会进入 `Invalid`，防止两个控制面抢占同一组节点。

Helm 按标准行为只在首次安装时创建 `crds/` 中的 CRD。升级 chart 前先更新 CRD schema：

```sh
kubectl apply -f deploy/helm/rustqueue/crds/rustqueue.io_rustqueueclusters.yaml
helm upgrade rustqueue deploy/helm/rustqueue --namespace rustqueue-system
```

查看状态与端点：

```sh
kubectl -n rustqueue-system get rq,pods,pvc,pdb
kubectl -n rustqueue-system describe rq rustqueue

# namespace 内客户端
rustqueue.rustqueue-system.svc:4150
http://rustqueue.rustqueue-system.svc:4151
```

OrbStack 单节点仅用于功能验收。它显式开启 3 个虚拟故障域，不能代表生产故障隔离：

```sh
make k8s-acceptance
```

验收会构建本地镜像、安装 Helm chart、启动三 Broker 虚拟 Cell、实际发布并消费消息、删除重建一个 Pod 并核对 PVC UID，最后执行一次自动滚动镜像升级。默认清理测试 namespace；`KEEP_CLUSTER=1 make k8s-acceptance` 可保留现场。

创建原生多 partition topic：

```sh
curl -X POST \
  'http://127.0.0.1:4151/topic/create?topic=events&partitions=8&replication_factor=3'
curl 'http://127.0.0.1:4151/v1/partitions?topic=events'
```

在线把已有 topic 从 4 个 partition 增加到 8 个：

```sh
curl -X POST -H 'content-type: application/json' \
  -d '{"target_partitions":8}' \
  'http://127.0.0.1:4151/v1/topics/events/partitions'
curl 'http://127.0.0.1:4151/v1/cluster/operations'
```

扩容只增加空 partition，不移动历史消息、timer、in-flight lease 或 ACK cursor。所有新 partition 完成 channel barrier 后在一条元数据命令中同时变为 `ACTIVE`。激活前可取消，激活后不能回滚。

`PUB`/`DPUB` 默认按 topic 轮询，`MPUB` 整批固定到同一 partition。HTTP 扩展可指定 `partition` 或稳定 CRC32C routing key：

```sh
curl -d 'ordered' \
  'http://127.0.0.1:4151/pub?topic=events&key=customer-42'
curl -d 'explicit' \
  'http://127.0.0.1:4151/pub?topic=events&partition=3'
```

routing key 只映射到 topic 创建时固定的永久 slot；在线扩容不会改变同一 key 所在 partition。新增 partition 接收轮询和显式路由流量。

## 节点与可靠性语义

- 4 节点仍使用 RF=3；失去任意一个节点时每个 partition 保留 2/3 quorum。
- 5 节点 RF=3 提升容量和并行度；RF=5 可同时失去任意两个节点，但写入需要 3/5 quorum。
- 发布只有在多数副本持久化并应用后才返回 `OK`。失去 quorum 时停止发布 ACK 和新投递。
- 消息交付前执行 quorum 线性化确认。FIN/REQ 也经过复制；未提交确认在故障后会重复投递，不会丢消息。
- in-flight timeout、RDY 和 TOUCH lease 是 Leader 内存态；Leader 切换后未持久确认的消息可立即重投。
- 中段 CRC、magic 或日志连续性错误会隔离 replica。健康 quorum 会把损坏 voter 降为 learner，quarantine 旧 generation，安装快照并重新提升。
- 只读文件系统、元数据组损坏或同组多个副本损坏需要人工处理。磁盘高水位先停止接收发布；仅在配置启用且整个集群持续无可用存储目标时，才执行上面所述的 quorum 保护性淘汰。
- `/ping` 只表示进程存活；`/v1/health` 只有在元数据、所有 ACTIVE partition、存储和时钟都可用时返回 200。

0.6 只接受全新 format v6 数据目录或 v6 快照恢复；检测到旧格式或无格式标记的旧布局会拒绝启动，不提供在线格式迁移。

## 在线滚动升级

format v6 内部帧和既有 `QueueCommand` tag 保持 append-only，新增能力只追加新命令并受持久化 feature level 门禁。节点通过内部 `/raft/time` 公告支持范围；混合版本期间，Cell、Catalog shard 或 topic 分别取作用域内节点的最低能力，只有全部节点都支持后才单调激活新 level。大消息和保护性淘汰在相应 level 激活前会 fail closed，不会把旧节点无法解码的 entry 写入 Raft。

滚动升级时逐 Cell、逐节点替换并等待该节点重新进入 `/v1/health` 与 `/lookup`，无需迁移 format v6 数据。feature level 一旦提高便不能回退到支持级别更低的二进制；此后故障处理应继续前滚。该约束只保证共享 format v6、RPC v6 和稳定 command schema 的相邻版本滚动，不承诺旧数据格式迁移。

## 运维 API

接口优先使用全局无歧义的 `group_key`；legacy `group_id` 只在当前拓扑中唯一时可用，省略时操作本 Cell metadata：

```sh
# 查看放置和本地/远端 replica
curl 'http://127.0.0.1:4151/v1/partitions?topic=events'
curl 'http://127.0.0.1:4151/v1/replicas'

# 所有 membership 类操作返回持久化 operation ID
curl -X POST \
  'http://127.0.0.1:4151/v1/cluster/transfer-leader?group_key=partition-1-4&node_id=2'
curl -X POST \
  'http://127.0.0.1:4151/v1/cluster/snapshot?group_key=partition-1-4'
curl -X POST -H 'content-type: application/json' \
  -d '{"group_key":"partition-1-4","voters":[2,3,4],"retain_removed_as_learners":true}' \
  'http://127.0.0.1:4151/v1/cluster/rebalance'

# 整节点 drain 和单 replica 重建
curl -X POST \
  'http://127.0.0.1:4151/v1/cluster/drain?node_id=1'
curl -X POST \
  'http://127.0.0.1:5151/v1/replicas/partition-1-4/3/repair'
curl -X POST \
  'http://127.0.0.1:5151/v1/storage/scrub'

# 查看可续跑操作、自动 rebalance 计划和部分可用的原生聚合统计
curl 'http://127.0.0.1:4151/v1/cluster/operations'
curl 'http://127.0.0.1:4151/v1/cluster/rebalance/plan'
curl 'http://127.0.0.1:4151/v1/stats'
```

启用 `[cluster.discovery]` 后，现有节点不需要预先配置新节点。新节点只需配置自身、初始元数据 voter 和至少一个 seed；局域网也可以启用 mDNS。节点公告由持久 PeerId 和共享 join token 认证，内部 Raft 地址还必须通过 mTLS 探测并返回相同 Node ID。发现结果写入元数据 Raft 后，新节点才会作为 learner 追平；发现本身永远不会直接改变 voter 或 partition membership。节点稳定 60 秒后才可成为新副本目标，自动控制器不会降低 RF，也不会在缺少安全候选时删除数据。

兼容 `/stats` 要求所有 ACTIVE group 都成功采集，否则返回 503；原生 `/v1/stats` 可以返回带 `complete`、`missing_groups` 和采集时间的部分结果。

## 安全与配置

完整配置见 `rustqueue.example.toml`。内部 Raft/RPC 强制 mTLS，并应与客户端证书使用不同信任域。P2P discovery join token 至少 32 字节，只证明节点有资格申请加入；最终节点目录仍由元数据 Raft 决定。开发 Compose 生成的证书和 token 只用于本机。

TCP AUTH 兼容 NSQ 外部 HTTP 授权响应。授权请求有连接/总超时、响应大小和 TTL cache 上限，服务异常时 fail closed；权限匹配使用 Rust 线性时间正则实现。

兼容 HTTP 发布端点应只监听受信网络，或启用 bearer token/mTLS。管理接口使用独立 bearer token。密钥和 token 使用文件引用，日志不会输出认证值。

## 快照与备份

三副本提高在线容错，但不能替代备份。离线导出会记录逐文件 CRC32C，并要求恢复目标为空：

```sh
docker compose down
docker run --rm -v rustqueue_rustqueue-data:/data -v "$PWD/backups:/backups" rustqueue:dev \
  snapshot export --data-path /data --snapshot-dir /backups --name nightly
docker run --rm -v "$PWD/backups:/backups" rustqueue:dev \
  snapshot verify --snapshot-dir /backups --name nightly
```

导出命令需要在数据目录写入并持有 `.rustqueue.lock`，因此卷必须可写；它不会修改消息、segment 或快照内容。若 broker 仍在运行，锁冲突会直接拒绝导出。

命名卷恢复时需先把卷根初始化为运行时 UID 65532 可写，再恢复到卷内子目录；启动恢复实例时将 `RUSTQUEUE_DATA_PATH` 指向该子目录。仓库提供的演练会创建临时卷，完成 export、verify、restore、重启和消息统计比对，并自动清理：

```sh
make snapshot-drill
```

生产发布前必须在实际卷和存储驱动上执行同等的恢复演练。

## 验收与基准

```sh
make fmt
make clippy
make test             # Rust 单元、property/invariant 和 OpenRaft storage suite
FUZZ_SECONDS=10 make fuzz-smoke
make compat           # 官方 Go/Python：核心命令、lookup、压缩、TLS/mTLS、AUTH
make acceptance-4     # RF=3、逐 group drain、Leader SIGKILL 续跑、repair、快照、指标
make acceptance-5     # RF=3/RF=5 故障矩阵，官方消费者逐条核对 ACK 账本和 missing=0
make acceptance-expand # 持续 PUB/SUB 下 4 -> 8，已有连接不重连且 missing=0
make acceptance-network-scale # 1024 partition 长轮询、内部请求放大和批量 ACK 门禁
make acceptance-9     # 9 节点放置均衡与单节点故障继续发布
make acceptance-federation # 3 Cell/9 节点跨 Cell ledger、migration 与 ephemeral
make acceptance-rolling # 5 节点连续发布消费下逐节点滚动替换，missing=0
make k8s-acceptance  # OrbStack K8s：Helm、消息流、PVC 重建、自动滚动升级
make rss-gate         # MPUB 填充 1000 万条 1 KiB，RSS 增量 <= 128 B/消息并持续采样资源
make snapshot-drill   # 离线导出、校验、恢复和重启比对
make crash-smoke      # 五节点轮换 SIGKILL 的快速消息账本门禁
DURATION_SECONDS=86400 make soak
make benchmark
```

`acceptance-4` 和 `acceptance-5` 会清空各自 Compose 项目的验收卷；设置 `KEEP_CLUSTER=1` 可在结束后保留现场。

`acceptance-network-scale` 在 4 节点 RF=3 集群创建 1024 个 partition，并用 32 个空闲消费者比较 Prometheus 前后快照。门禁要求外部 fetch 保持在长轮询频率范围内，内部 fetch 不随 partition 数相乘；随后向单 partition 原子 MPUB 64 条消息，逐条核对消费账本与批量 FIN。相关指标包括 `rustqueue_consumer_fetch_*`、`rustqueue_internal_rpc_*`、`rustqueue_fetch_batch_messages`、`rustqueue_ack_batch_messages`、redirect 和 retry counter。

`rss-gate` 默认用 64 条一批的 MPUB 填充 backlog；`BATCH_SIZE=1` 可退回逐条 PUB。门禁会在 `benchmarks/results/` 同时写出 `docker stats` 的 CPU、RSS、网络、块 I/O、PID 采样和精确 `/proc` RSS 峰值序列。

soak harness 默认每 15 分钟轮换对节点执行 `SIGKILL` 并从同一卷重启；可用 `RESTART_INTERVAL_SECONDS` 调整间隔，或设置 `RESTART_MODE=graceful` 改为正常重启。它按 message sequence 记录 acknowledged、consumed unique、duplicate、missing、unconfirmed 和 errors；发布已确认集合中的 `missing` 必须为 0。至少一次语义允许 duplicate。

正式基准默认覆盖 100 B、1 KiB、10 KiB，16 producers、16 consumers、60 秒预热、10 分钟测量、连续 3 次取中位数。它分别报告：

- RustQueue 单节点 durable；
- 5 节点部署中的多 partition RF=3 quorum-durable；
- NSQ `mem-queue-size=0,sync-every=1` 严格持久化；
- NSQ 默认风格 `sync-every=2500`，单独展示而不混用结论。

固定到达率模式按计划到达时间计算延迟，以避免 coordinated omission。正式目标是 1 KiB RF=3 quorum-durable 吞吐达到 NSQ 严格持久化基线的至少 2 倍，且发布 ACK p99 不更差；只能以完整生成报告判定，不能降低副本数或持久化级别。

## 端口

| 端口 | 用途 |
| --- | --- |
| 4150 | NSQ V2 TCP |
| 4151 | HTTP、lookup、metrics、管理接口 |
| 4250 | OpenRaft 内部 mTLS，仅节点间开放 |
