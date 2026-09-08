# Java + Redis 实验网关

这是本地真实终端使用的 Java 8 网关。它通过 TCP 和 RESP 协议直接连接 Redis，不生成模拟命令结果。

## 启动

前提：`javac` 指向 JDK 8 或更高版本。

```sh
docker compose -f server/docker-compose.yml up -d
npm run backend
```

默认地址：

- 网关：`http://127.0.0.1:8787`
- Redis：`127.0.0.1:6379`

JVM 实机观测使用 `GET /api/jvm?probe=<name>`。允许的固定探针为
`runtime`、`bytecode`、`classloaders`、`memory`、`allocation`、
`references` 和 `gc`。探针不接受用户 Java 源码、类名、JVM 参数或
系统命令。

环境变量：

- `LAB_HTTP_PORT`：网关端口，默认 `8787`
- `LAB_REDIS_HOST`：Redis 主机，默认 `127.0.0.1`
- `LAB_REDIS_PORT`：Redis 端口，默认 `6379`
- `LAB_ALLOWED_ORIGIN`：允许的前端 Origin；默认 `local`，仅接受 localhost 和 127.0.0.1

如果前端使用其他网关地址，构建前设置 `VITE_REDIS_LAB_API`。

## 安全边界

- HTTP 服务只监听 `127.0.0.1`。
- 客户端必须提供 8-64 位会话标识。
- 所有 Key 自动改写为 `lab:<session>:<key>`。
- 命令和参数数量均有白名单限制。
- 禁止任意 Lua、配置、全库扫描、监控和清空数据库命令。
- 请求体最大 512 字符；单个会话最多跟踪 200 个 Key 用于一键重置。
- 真实公网部署还必须增加 HTTPS、身份认证、配额、审计和进程级沙箱；当前实现只用于本机学习。

允许的命令：`PING`、`SET`、`GET`、`DEL`、`MGET`、`EXISTS`、`INCR`、`DECR`、`HSET`、`HGET`、`LPUSH`、`RPUSH`、`LRANGE`、`SADD`、`SMEMBERS`、`ZADD`、`ZRANGE`、`EXPIRE`、`TTL`、`PTTL`、`TYPE`。

## 测试

```sh
npm run test:java
```

该测试验证命令解析、Key 命名空间、参数数量和危险命令拒绝逻辑，不需要 Redis。
