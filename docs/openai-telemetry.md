# OpenAI 指标采样

指标合同以 [Codex `98f8a3311ba7`](https://github.com/openai/codex/tree/98f8a3311ba7cac785e570713127e9127af4043a/codex-rs/otel) 为准
集中目录的 82 项均有采样入口，包含真实传输采样、客户端事实和缺失本地测量时的模拟
这不代表每次请求都会产生 82 项，也不包含官方其他模块分散定义的全部指标
开关、出口、认证和 Analytics 事件见[部署文档](../deploy/README.md#codex-传输遥测)

## 来源与边界

| 范围 | 来源与触发条件 |
| --- | --- |
| API、SSE、WebSocket 与引擎 timing | 实际收发状态和上游 timing 事件 |
| reasoning_replay、reasoning_items_removed | 完成响应的 `input_transformations`，报告存在时包含六类原因的零值样本 |
| websocket.continuation | 实际请求的 `previous_response_id`、`generate` 及明确提供的续接诊断 |
| 轮次耗时、首 Token、首消息、Token、成本和工具数 | 按客户端轮次关联多次采样，按实际响应模型汇总用量，明确提供的轮次事实优先 |
| 工具注册与增量目录 | 读取请求可见定义，排除托管工具，完整目录和渲染尺寸可由客户端补充 |
| 本地工具、exec、Goal、多代理 | 关联已观察到的调用与后续工具结果；状态指标要求明确的执行结果，未知状态的回传仍计入轮次工具完成数 |
| 启动、线程、Skills | 首次观察到对应虚拟客户端或线程时采样；启动时长及缺失本地目录使用模拟画像 |
| Hook、Artifact、插件同步、Guardian | 提供相应事实时触发；缺失本地时长或数量时使用对应模拟值 |
| 其余客户端指标事实 | `telemetry.events` 接收限定的官方名称、类型和维度，按事件 ID 去重 |

模拟值与实测值沿用相同官方指标名和标签，不增加自定义来源标签
因此，接收端不能仅凭一个指标点区分其来源，分析时需结合客户端提供了哪些事实
模拟画像不会额外执行工具、启动进程、安装插件或创建 Goal

客户端提供的设备与机器标识在出站协议元数据中使用账号固定派生值，规则见[请求头与身份](api.md#请求头与身份)
同一账号跨 Key、UA 和服务重启保持相同设备 UUID，不取客户端设备值，也不使用 Key 的安装种子派生设备 UUID
Analytics 与 Metrics 沿用官方字段结构，没有对应设备字段时不额外添加；会话安装身份与设备身份分开处理

网关看不到客户端启动前的真实计时，虚拟启动阶段为 `configure_session`，缺少测量时使用 1 ms
缺少本地 Skills 目录时模拟为空目录；未提供的内存、托管网络代理和后台进程状态使用关闭或零的模拟画像
工具结构化回传中的布尔 `success`、`is_error` 或非 `null` 的 `error` 用于确认执行状态，明确失败优先；仅有正文、内容数组或 `error: null` 时不推断成功
官方工具输出的内部 `success` 不随正文序列化，因此普通客户端回传可能无法产生执行状态指标；客户端也可通过 `telemetry.events` 提供明确事实
执行状态明确时，工具回传包含 `duration_ms` 就使用该值，否则从调用返回到结果回传的间隔模拟工具耗时，其中包含调度和传输
工具渲染尺寸缺省按限定名称、描述 UTF-8 字节及固定声明开销估算，不把结构化 JSON 字节声称为官方渲染结果
关键路径缺省以已观察到的模型采样作为 `inference`，其余可见时间放入 `unknown`，其他本地阶段模拟为零

## 轮次关联

`session_id`、`thread_id`、`turn_id` 使用经过出站身份处理的请求值，优先级与 Analytics 元数据一致
不会生成新的客户端 `turn_id`，缺少轮次身份时只能用当前网关请求作为内部关联范围
同一轮次中的多次模型响应按响应 ID 去重，Token 按实际产出模型汇总后各记录六种 `token_type`
目录注册按采样请求记录，传输重试不重复；客户端可用 `sampling_request_id` 明确标识同一次采样

工具调用后保留轮次等待下一次采样，普通完成响应且没有待回传的本地工具时推断为轮次结束
客户端可以通过 `telemetry.turn.completed` 明确控制结束条件，提供完整计时时覆盖代理可见区间
客户端事实不完整时，这一边界属于虚拟轮次，无法覆盖所有客户端排队、用户等待和后台工作
空闲状态有容量和 30 分钟保留上限，由现有导出 daemon 清理，不创建额外后台任务

成本优先读取 `telemetry.turn.cost_microusd`
缺省时使用真实 Token 和 Provider 内置公开价目计算不含网关倍率的估值，按轮次累加后四舍五入到微美元
缺少任一响应的用量或价格时不生成部分估值，明确的 `null` 不回填估值
这与官方服务端费用结算存在来源差别，不能解释为 OAuth 账号实际结算金额

## 客户端事实合同

在请求的 `client_metadata.telemetry` 中提供事实，元数据来源优先级与 Analytics 相同
按官方 `HashMap<String, String>` 合同，`telemetry` 的传输值为 JSON 字符串
例如以下 JavaScript 对象可直接作为请求的 `client_metadata`：

```javascript
const clientMetadata = {
    "session_id": "session-from-client",
    "thread_id": "thread-from-client",
    "turn_id": "turn-from-client",
    "sampling_request_id": "sampling-from-client",
    "telemetry": JSON.stringify({
      "turn": {
        "completed": true,
        "e2e_duration_ms": 1200,
        "ttft_ms": 180,
        "ttfm_ms": 420,
        "cost_microusd": 150,
        "network_proxy_active": false,
        "memory_feature_enabled": false,
        "config_use_memories": false,
        "has_memory_citations": false,
        "running_processes": 0
      },
      "tools": {
        "tool_mode": "code_mode",
        "incremental_tools": true,
        "fragment_bytes": {"before": 1800, "after": 1600},
        "namespaces_total": {"before": 3, "after": 3},
        "registrations": {"direct": 2, "code_mode_only": 8}
      },
      "skills": {
        "enabled_total": 4,
        "kept_total": 3,
        "description_truncated_chars": 100,
        "catalog_surface": "thread_context"
      },
      "events": [
        {
          "id": "hook-run-from-client",
          "name": "codex.hooks.run",
          "value": 1,
          "tags": {
            "hook_name": "PreToolUse",
            "source": "plugin",
            "status": "completed",
            "handler_type": "command",
            "execution_mode": "sync"
          }
        }
      ]
    })
};
```

`process_id` 可放在 `client_metadata` 中明确标识同一客户端进程，未提供时按会话模拟进程边界
`service_name` 是可选公共指标标签；产品归因读取实际出站 `x-openai-product-sku`，只区分 `codex` 和 `other`
`telemetry.websocket_continuation` 可补充 `reason`、`previous_item_type`、`current_item_type` 和 `after_prewarm`
是否增量仍由实际 `previous_response_id` 决定，是否预热仍由实际 `generate: false` 决定
`telemetry.websocket_connect.reauth` 可提供客户端明确的认证恢复状态，缺省使用 `false`

`telemetry` 中还接受以下结构化事实：

| 字段 | 内容 |
| --- | --- |
| `turn.critical_path` | `duration_ms`、`inference`、`auto_review`、`tools`、`user_wait`、`overhead`、`unknown`，数值均为毫秒；`status` 为 `complete`、`incomplete` 或 `disabled`，后两者只记录采集状态 |
| `turn.tmp_mem_enabled`、`turn.tool_calls` | 临时内存状态与整轮已完成工具数量 |
| `thread` | `is_git`、`is_worktree` |
| `startup.phases[]` | `phase`、`status`、`duration_ms` |
| `additional_context[]` | `kind`、可选 `name`、`value` 或 `before_bytes`、`after_bytes`，字节不包含消息包装 |
| `hooks[]` | 本次新增的已完成 Hook 事实，维度同示例，可附 `duration_ms` |
| `artifact_operation` | `skill`、`artifact_type`、`operation_kind`、`output_format`、`execution_backend`、`expected_output_count` |
| `plugin_sync` | `transport`、`status` |
| `guardian` | 审查结果维度、`duration_ms`、`ttft_ms`，以及 `truncations[]` 的 `component`、`original_bytes`、`retained_bytes` |

结构化事实描述本次新增事实，重放历史事件时使用 `events[].id` 关联和去重
`events` 每次最多接收 256 条，`id` 最长 256 字节；相同名称和 ID 在同一账号客户端线程内去重
没有 ID 时仅在同一采样请求内去重，不根据相同数值合并不同时刻的独立事件
事件值必须为非负有限数，计数器与普通数量直方图使用整数；未知名称、未知标签和不合法的值被忽略
明确提供的有效事件优先于本次同名模拟指标，官方 Statsig 过滤同样适用

`events` 的名称范围为以下客户端指标族中的集中目录条目：

- `codex.artifact.operation.*`、`codex.exec_command.*`、`codex.tool.call*`、`codex.tool.unified_exec`、`exec_server_client_requests_total`
- `codex.goal.*`、`codex.guardian.review*`、`codex.hooks.run*`、`codex.multi_agent.*`、`codex.plugins.*`
- `codex.process.start`、`codex.thread.started`、`codex.startup.phase.duration_ms`、`codex.startup_prewarm.*`、`codex.thread.skills.*`

名称是精确白名单，不接受这些前缀下的任意新名称
普通数量、Token、字节、Goal 秒数使用官方整数直方图口径，耗时使用毫秒直方图，exec 总耗时和成本使用可变增量计数器
Statsig 继续过滤 API 请求、部分引擎指标、exec-server RPC、工具调用及轮次 Token 和成本，这些指标可发往显式 OTLP 收集器
Analytics 仍只包含网页搜索和图片生成两个已实现事件，这里的指标覆盖不代表新增了其他 Analytics 事件
