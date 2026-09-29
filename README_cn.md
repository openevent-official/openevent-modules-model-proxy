# OpenEvent Model Proxy

[English version](README.md)

OpenEvent Model Proxy 是一个基于 OpenEvent 的模型调用代理。它通过 `llm.v1`
事件协议接收模型请求，调用 OpenAI-compatible provider，并把结果写回同一个
OpenEvent channel。

## 项目结构

- `src/openevent/model_proxy/`：`model-proxy` worker 和命令行入口
- `src/openevent/model_proxy_sdk/`：`llm.v1` payload 构造、校验、解析和发布工具
- `openevent.model_proxy_sdk.OpenAI`：支持普通和流式调用的 OpenAI-like 同步客户端
- `docs/`：面向使用者和贡献者的公开文档
- `tests/`：基于 Python `unittest` 的测试

## 支持范围

- 协议版本：`llm.v1`
- Python 版本：`>=3.10`
- OpenEvent SDK：`openevent-sdk>=0.11.1`
- YAML 解析：`PyYAML>=6.0`
- Provider 类型：`openai_compatible`，兼容要求见 [llm.v1 协议](docs/LLM_PROTOCOL_cn.md#41-openai_compatible-的最小兼容契约)
- Endpoint：`POST /v1/chat/completions`、`POST /v1/responses`，均支持普通和流式调用
- Python API：协议消息构造、解析与发布，以及 OpenAI-like 同步客户端

## 构建和测试

构建、测试和安装使用 `make`；wheel 放在 `dist/`，临时文件、构建依赖和缓存放在 `build/`。
先在目标 Python 环境安装满足上述版本要求的 SDK：

```bash
cd openevent-sdk
make install
cd ..
```

常用命令：

```bash
make build    # 只构建
make install  # 构建并安装
make clean    # 清理 dist/ 和 build/
```

构建清理本项目旧 wheel，安装和 e2e 只使用本次成功构建的产物。安装先按 wheel 声明补齐依赖，复用满足要求的版本，
再替换本项目包，即使版本号相同也会生效；依赖解析失败时不替换本项目。用 `PYTHON` 选择 Python 环境，
`INSTALL_ARGS` 可传入 `--find-links` 等索引和网络选项，不能改变安装位置或依赖替换策略。

测试使用当前环境已安装的 SDK，不从子模块安装；测试工具 `packaging` 用于版本检查：

```bash
python3 -B -m pip install packaging
make test
```

端到端测试需显式指定 OpenEvent server 二进制：

```bash
OPENEVENT_SERVER_BIN=<openevent_server_binary> make e2e
```

SDK 缺失或版本不满足时，e2e 在启动 OpenEvent 前报错退出。测试进程由 GNU `timeout` 限时，默认 `120` 秒，
可通过 `E2E_TIMEOUT_SECONDS` 调整；构建和安装不计入限时。

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

## 文档

| 文档 | 内容 |
| --- | --- |
| [docs/LLM_PROTOCOL_cn.md](docs/LLM_PROTOCOL_cn.md) | `llm.v1` payload 字段、消息状态机、状态码、响应头转发、payload 大小行为和对外恢复契约 |
| [docs/CONFIGURATION_cn.md](docs/CONFIGURATION_cn.md) | Worker 配置、provider URL 和 timeout |
| [docs/OPEN_EVENT_RPC_RETRY_cn.md](docs/OPEN_EVENT_RPC_RETRY_cn.md) | GetStatus、Fetch、UUID 分配与查询、Subscribe 等普通 OpenEvent RPC 的统一重试规则 |
| [docs/RESULT_PUBLISHING_cn.md](docs/RESULT_PUBLISHING_cn.md) | 单条事件的 UUID 冻结、可靠发布、UUID 对账和 `ResultPublishError` 提交状态 |
| [docs/SDK_API_cn.md](docs/SDK_API_cn.md) | Python SDK 的公开签名、返回对象、异常和生命周期 |
| [docs/SDK_USAGE_cn.md](docs/SDK_USAGE_cn.md) | Python SDK 使用示例 |
