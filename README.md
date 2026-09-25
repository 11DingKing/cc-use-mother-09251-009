# 跨平台充电状态订阅

面向多家导航/出行平台的充电服务区状态**订阅交付服务**（纯服务端，零第三方依赖）。
各合作方拥有不同的可接收字段、刷新频率与区域范围；突发拥堵事件优先送达。

## 能力一览

- **合作方授权**：合作方档案、API 密钥（仅存 SHA-256 哈希）、多密钥共存、密钥轮换、过期/吊销、运营暂停/恢复、授权撤销。
- **版本化过滤规则**：区域集合、字段白名单、最低拥堵等级、站点白名单；每次区域/字段调整生成新版本并保留历史，存量批次始终按其生成时的规则版本交付。
- **版本化增量包**：站点事件以全局单调 `seq` 幂等摄入（重复 `event_id` 去重），按合作方游标与当前规则投影生成增量批次，包内事件严格按序。
- **交付租约**：拉取/派发即占租约（带 TTL 与属主令牌），确认必须持当前属主令牌；超时回收后同一批次按原顺序重投（`delivery_attempt+1`），失败批次不跳过。
- **幂等计费与确认**：`(subscription_id, event_id)` 指纹作为计费主键，重投、回放、重复确认都不会重复计费；同属主重复确认幂等成功，旧属主/伪造令牌确认被拒。
- **拥堵优先**：批次携带 `priority`，待投递队列为「高优先级优先、同级 FIFO」，突发拥堵抢占队首。
- **撤销即止血**：撤销授权后密钥立即失效、存活批次退出投递，且该合作方**全部历史批次**（含已确认）中的敏感字段（`operator_note`、`internal_code`、`repair_contact`）在 SQLite 中原地脱敏。
- **运营能力**：按序列回放（游标回退 + 旧批次作废）、暂停/恢复、完整审计日志（授权、密钥、规则、拉取、重投、确认、回放、撤销）。
- **持久化恢复**：事件、授权、规则历史、游标、发件箱、租约、计费、审计全部落 SQLite（WAL）；重启后游标与未确认批次、有效租约均可恢复。

## 分层结构

```
service_09251_009/
├── domain/models.py          领域模型与不变量（事件/合作方/规则/批次/游标/审计）
├── ports/clock.py            可替换时钟与标识端口（FixedClock/顺序ID 便于测试）
├── storage/sqlite_store.py   SQLite 仓储 + 部分唯一索引（每订阅至多一个存活批次）
├── services/subscription_service.py  应用编排：授权、过滤、游标、租约、计费、回放
├── api/http_api.py           HTTP/JSON 边界（合作方接口 + 运营接口）
├── app.py                    装配工厂 build_service(db_path, ...)
└── __main__.py               CLI 启动
```

关键并发保证：存储层单连接 + 写事务串行化；确认走单条条件 UPDATE（`state='delivered' AND lease_owner=?`）原子完成状态翻转与游标推进；应用层另设订阅级锁避免并发构建重复批次。

## HTTP 接口

合作方（请求头 `X-Partner-Id` + `Authorization: Bearer <secret>`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/v1/subscriptions/{id}/batch` | 拉取增量包（租约内幂等；受刷新频率约束） |
| POST | `/v1/subscriptions/{id}/ack` | 持 `lease_owner` 确认批次，推进游标 |

运营方（`Authorization: Bearer <operator_token>`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/operator/partners` | 注册合作方并签发初始密钥 |
| POST | `/v1/operator/partners/{id}/keys` | 签发新密钥（支持重叠轮换） |
| POST | `/v1/operator/partners/{id}/rotate` | 签发新密钥并作废旧密钥 |
| POST | `/v1/operator/partners/{id}/revoke` | 撤销授权并脱敏历史敏感字段 |
| POST | `/v1/operator/partners/{id}/suspend` `/resume` | 暂停 / 恢复 |
| POST | `/v1/operator/subscriptions` | 创建订阅（刷新频率/批量/区域/字段/拥堵阈值） |
| POST | `/v1/operator/subscriptions/{id}/rules` | 调整规则（版本化；省略的维度保持上一版，显式 `null` 放开） |
| POST | `/v1/operator/subscriptions/{id}/replay` | 回放（`from_seq`） |
| GET | `/v1/operator/queue` | 待投递队列（拥堵优先） |
| POST | `/v1/operator/dispatch` | 按优先级派发队首批次（推送出口） |
| GET | `/v1/operator/cursors/{id}` | 查看游标 |
| GET | `/v1/operator/audit` | 审计查询（可按合作方/订阅/动作过滤） |
| POST | `/v1/internal/events` | 站点事件摄入（幂等） |

## 运行

```bash
# 数据库默认落在系统临时目录，可用 --db 或 CHARGE_SUB_DB 指定（不写入源码目录）
python3 -m service_09251_009 --host 127.0.0.1 --port 8080 \
    --operator-token "$CHARGE_SUB_OP_TOKEN"
```

也可在代码中直接装配：

```python
from service_09251_009.app import build_service

service, store = build_service("/var/lib/charge/sub.db", lease_ttl=30)
```

## 测试

覆盖慢消费者与租约超时重投、密钥轮换与过期、区域调整版本化、并发确认/拉取竞争、
重启恢复（游标/发件箱/租约/计费）、撤销后历史敏感字段脱敏、拥堵优先级、回放与审计。

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q service_09251_009 tests
```
