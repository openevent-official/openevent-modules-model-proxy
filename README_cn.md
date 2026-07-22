# OpenEvent Model Proxy

[English version](README.md)

OpenEvent Model Proxy 是一个基于 OpenEvent 的模型调用代理。它通过 `llm.v1`
事件协议接收模型请求，调用 OpenAI-compatible provider，并把结果写回同一个
OpenEvent channel。

本仓库包含：

- `src/openevent/model_proxy/`：`model-proxy` worker 和命令行入口
- `src/openevent/model_proxy_sdk/`：`llm.v1` payload 构造、校验、解析和发布工具
- `openevent.model_proxy_sdk.OpenAI`：OpenAI-like 同步非流式客户端
- `docs/`：面向使用者和贡献者的公开文档
- `tests/`：基于 Python `unittest` 的测试

## 项目状态

- 协议版本：`llm.v1`
- Python 版本：`>=3.10`
- OpenEvent SDK：`openevent-sdk>=0.4.0`
- YAML 解析：`PyYAML>=6.0`
- Provider 类型：`openai_compatible`
- Provider 默认 allowlist：`POST /v1/chat/completions` 和 `POST /v1/responses`；
  其他 method 或 path 必须显式配置
- 流式响应：暂不支持 `stream=True`

## 构建和测试

构建、测试和安装统一通过 `make` 执行。最终 wheel 产物放在 `dist/`。
`build/` 是保留的临时目录，只放构建依赖、测试临时文件、缓存和临时文件。

本项目通过 Python 包依赖使用 `openevent-sdk>=0.4.0`。测试使用当前 Python 环境中
已经安装好的 SDK 包，不会从 `openevent-sdk/` 子模块安装 SDK。

正常运行、构建或安装时，请先把 SDK 安装到目标 Python 环境：

```bash
cd openevent-sdk
make install
cd ..
```

需要把 SDK 安装到自定义目录时，通过 SDK 子模块的 `INSTALL_ARGS` 传递
`pip install` 参数：

```bash
cd openevent-sdk
make install INSTALL_ARGS="--target /opt/openevent-sdk"
```

只构建、不安装到当前 Python 环境：

```bash
make build
```

构建完成后，wheel 位于 `dist/`。

构建并安装生成的 wheel 到当前 Python 环境：

```bash
make install
```

需要指定安装路径时，通过 `INSTALL_ARGS` 传递 `pip install` 参数：

```bash
make install INSTALL_ARGS="--target /opt/openevent-model-proxy"
make install INSTALL_ARGS="--prefix /opt/openevent-model-proxy"
```

运行测试：

```bash
make test
```

运行端到端测试时，需要显式传入 OpenEvent server 二进制：

```bash
OPENEVENT_SERVER_BIN=<openevent_server_binary> make e2e
```

如果 SDK 缺失或版本不满足，e2e 脚本会在启动 OpenEvent 前直接退出报错。

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
python3 -m openevent.model_proxy.cli --config model-proxy.yaml
```

配置文件示例见 [docs/CONFIGURATION_cn.md](docs/CONFIGURATION_cn.md)。

## Python SDK

OpenAI-like 客户端和底层协议 SDK 用法见 [docs/SDK_USAGE_cn.md](docs/SDK_USAGE_cn.md)。

## 文档

- [docs/CONFIGURATION_cn.md](docs/CONFIGURATION_cn.md)：worker 配置文件
- [docs/SDK_USAGE_cn.md](docs/SDK_USAGE_cn.md)：Python SDK 使用方式
- [docs/LLM_PROTOCOL_cn.md](docs/LLM_PROTOCOL_cn.md)：`llm.v1` 事件协议
- [docs/RESULT_PUBLISHING.md](docs/RESULT_PUBLISHING.md)：worker result 发布可靠性
