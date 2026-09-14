# OpenEvent Model Proxy

[English version](README.md)

OpenEvent Model Proxy 是一个基于 OpenEvent 的模型调用代理。它通过 `llm.v1`
事件协议接收模型请求，调用 OpenAI-compatible provider，并把结果写回同一个
OpenEvent channel。

本仓库包含：

- `src/openevent/model_proxy/`：`model-proxy` worker 和命令行入口
- `src/openevent/model_proxy_sdk/`：`llm.v1` payload 构造、校验、解析和发布工具
- `openevent.model_proxy_sdk.OpenAI`：支持普通和流式调用的 OpenAI-like 同步客户端
- `docs/`：面向使用者和贡献者的公开文档
- `tests/`：基于 Python `unittest` 的测试

## 项目状态

- 协议版本：`llm.v1`
- Python 版本：`>=3.10`
- OpenEvent SDK：`openevent-sdk>=0.8.0`
- YAML 解析：`PyYAML>=6.0`
- Provider 类型：`openai_compatible`
- request 可以通过顶层可选的 `provider` 字段选择已配置的 provider；不传时使用
  `default_provider`
- 支持的 endpoint 固定为 `POST /v1/chat/completions` 和 `POST /v1/responses`；`openai_compatible`
  只表示 Provider 能按 [llm.v1 最小兼容契约](docs/LLM_PROTOCOL_cn.md#41-openai_compatible-的最小兼容契约)
  完成普通或流式请求和响应传输，不表示支持 OpenAI API 的全部模型、字段或产品能力；其他 method 或 path 都是非法 request
- 支持普通和流式结果；准确的消息形态、状态码和终态规则见协议文档

## 构建和测试

构建、测试和安装统一通过 `make` 执行。最终 wheel 产物放在 `dist/`。
`build/` 是保留的临时目录，只放构建依赖、测试临时文件、缓存和临时文件。

本项目通过 Python 包依赖使用 `openevent-sdk>=0.8.0`。测试使用当前 Python 环境中
已经安装好的 SDK 包，不会从 `openevent-sdk/` 子模块安装 SDK。

正常运行、构建或安装时，请先把 SDK 安装到目标 Python 环境：

```bash
cd openevent-sdk
make install
cd ..
```

只构建、不安装到当前 Python 环境：

```bash
make build
```

构建开始时会清理 `dist/` 中本项目的旧 wheel，保留其他包的产物。构建成功后，安装和 e2e 使用本次生成的 wheel；
构建失败时直接结束，不使用旧版本继续安装或测试。

构建并安装生成的 wheel 到当前 Python 环境：

```bash
make install
```

测试还需要 `test` extra 声明的 `packaging`，用于按 Python 包版本规则检查已安装的 SDK。
先安装这项测试工具，再运行测试：

```bash
python3 -B -m pip install packaging
make test
```

运行端到端测试时，需要显式传入 OpenEvent server 二进制：

```bash
OPENEVENT_SERVER_BIN=<openevent_server_binary> make e2e
```

如果 SDK 缺失或版本不满足，e2e 脚本会在启动 OpenEvent 前直接退出报错。
E2E 使用 GNU `timeout` 限制测试进程的运行时间，默认 `120` 秒，可通过 `E2E_TIMEOUT_SECONDS` 调整；前面的构建和安装不计入该时间。

清理构建产物和临时文件：

```bash
make clean
```

## 运行

启动 worker：

```bash
model-proxy --config model-proxy.yaml
```

也可以从源码运行：

```bash
PYTHONPATH=src python3 -B -m openevent.model_proxy.cli --config model-proxy.yaml
```

配置文件示例见 [docs/CONFIGURATION_cn.md](docs/CONFIGURATION_cn.md)。

## Python SDK

公开 Python API 见 [docs/SDK_API_cn.md](docs/SDK_API_cn.md)，常用示例见
[docs/SDK_USAGE_cn.md](docs/SDK_USAGE_cn.md)。

## 文档

每类设计规则只有一份权威文档。README 和 SDK 使用指南只提供入口与示例，不重新定义协议状态或可靠性规则。

| 文档 | 权威范围 |
| --- | --- |
| [docs/LLM_PROTOCOL_cn.md](docs/LLM_PROTOCOL_cn.md) | `llm.v1` payload 字段、消息状态机、状态码、响应头转发、payload 大小行为和对外恢复契约 |
| [docs/CONFIGURATION_cn.md](docs/CONFIGURATION_cn.md) | Worker 配置、provider URL 和 timeout |
| [docs/OPEN_EVENT_RPC_RETRY_cn.md](docs/OPEN_EVENT_RPC_RETRY_cn.md) | GetStatus、Fetch、UUID 分配与查询、Subscribe 等普通 OpenEvent RPC 的统一重试规则 |
| [docs/RESULT_PUBLISHING_cn.md](docs/RESULT_PUBLISHING_cn.md) | 单条事件的 UUID 冻结、可靠发布、UUID 对账和 `ResultPublishError` 提交状态 |
| [docs/SDK_API_cn.md](docs/SDK_API_cn.md) | Python SDK 的公开签名、返回对象、异常和生命周期 |
| [docs/SDK_USAGE_cn.md](docs/SDK_USAGE_cn.md) | 非权威的 Python SDK 示例和使用说明 |
