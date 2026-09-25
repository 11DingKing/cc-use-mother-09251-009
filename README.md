# 跨平台充电状态订阅

本项目用于建设面向业务人员的纯服务端系统。代码按领域模型、应用服务、持久化与接口边界组织；时间、标识和外部输入应通过可替换端口接入，以便稳定复现状态变化。运行数据与本地配置不得写入源码目录。

多家导航与出行平台通过本服务订阅充电服务区状态：各合作方的可见字段、刷新频率与区域范围按订阅配置，突发拥堵事件（高优先级）突破刷新频率限制优先送达。

## 结构

```
service_09251_009/
  ports.py     可替换端口：时钟、标识、密钥材料（测试注入确定性实现）
  domain.py    领域常量、业务错误、字段白名单投影
  storage.py   SQLite 持久化：授权/密钥、订阅与游标、版本日志、发件箱批次、计费、审计
  service.py   应用服务：授权与密钥轮换、过滤规则、摄入、拉取/租约、确认、回放、审计
  api.py       HTTP 接口边界（标准库 http.server）
  __main__.py  服务入口
```

## 核心语义

- **版本化增量包**：站点事件写入全局递增版本日志（`event_id` 幂等，重复摄入不重复入日志）；订阅按游标切出 `(version_from, version_to]` 批次，批次内事件引用快照存于发件箱。
- **游标与租约**：拉取即建租约；确认后游标推进。租约过期未确认 → 同批次同顺序重投（`attempt` 递增）；租约未到期重拉幂等返回同批次。慢消费者不乱序、不丢批。
- **幂等计费与确认**：计费按 `(订阅, 事件)` 唯一约束入账，确认按批次状态机幂等——相同事件不会重复计费或重复确认，回放重投亦不重复计费。
- **授权撤销**：字段在交付时按订阅当前白名单渲染；撤销订阅即在途批次作废、拉取与确认均拒绝，历史敏感字段不再暴露。密钥只存散列，轮换有宽限期，可即时吊销。
- **过滤规则**：区域白名单、字段白名单、刷新频率、批次大小均可按订阅调整；区域调整作废旧批次并按新区域重切，字段调整对在途批次重投立即生效。
- **运营**：回放（游标回退重投）、暂停/恢复、撤销、审计轨迹与计费台账均可查询。
- **恢复**：游标、发件箱、租约、计费与审计全部落 SQLite，重启后原样恢复。

## 运行

```bash
SVC_ADMIN_TOKEN=<运营令牌> python3 -m service_09251_009 --db /path/to/subscription.db --port 8080
```

数据库路径也可用 `SVC_DB_PATH` 指定；默认写入系统临时目录，不污染源码目录。

## 接口摘要

合作方（`X-Api-Key` 或 `Authorization: Bearer`）：

- `POST /v1/pull` `{subscription_id}` → 拉取下一批增量（含租约到期时间）
- `POST /v1/ack` `{subscription_id, batch_id}` → 确认批次（幂等）

运营方（`X-Admin-Token`）：

- `POST /admin/partners`、`POST /admin/partners/{id}/keys/rotate`、`POST /admin/keys/{id}/revoke`、`GET /admin/partners/{id}/keys`
- `POST /admin/partners/{id}/suspend|reactivate`
- `POST /admin/subscriptions`、`POST /admin/subscriptions/{id}/rules`
- `POST /admin/subscriptions/{id}/pause|resume|revoke|replay`
- `POST /admin/events`（摄入站点事件，幂等）
- `GET /admin/audit`、`GET /admin/billing`、`GET /admin/subscriptions/{id}/batches`

错误统一为 `{"error": {"code", "message"}}`，如 `rate_limited`、`lease_held`、`batch_superseded`、`out_of_order`。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖：慢消费者租约重投、密钥轮换宽限、区域调整、并发确认/拉取/摄入、重启恢复、撤销后敏感字段隔离、回放不重复计费、HTTP 端到端。

## 编译检查

```bash
python3 -m compileall -q service_09251_009 tests
```
