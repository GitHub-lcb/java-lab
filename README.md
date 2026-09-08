# Java Lab

面向 Java 后端开发者的交互式原理实验室。项目通过架构视图、可调参数、逐步日志、对照任务和验证题，把 Java、JVM、Redis 与常用中间件知识组织成可以实际操作的学习路径。

界面布局参考了 [liziwuli.com](https://liziwuli.com/) 的实验式学习思路，代码、视觉设计和教学内容均为独立实现，没有复制原站源码、品牌或课程内容。

> 当前阶段：内容持续建设中。Redis 基础里程碑和 JVM 基础里程碑已经完成，JVM 诊断路径正在建设。浏览器模拟不是生产性能测试；本地 Java 网关不能直接暴露到公网。

## 项目特点

- **实验驱动**：每课包含架构视图、参数控制、逐步演示、日志和 Java 教学代码。
- **完成标准明确**：实验、指定对照场景和原理验证全部通过后，课程才计为掌握。
- **模拟边界透明**：确定性模型只解释因果关系，不把操作计数包装成真实 QPS、延迟或 GC 停顿。
- **真实环境可验证**：通过本机 Java 网关运行受控 Redis 命令和固定 JDK 探针，展示真实返回值。
- **学习闭环**：阶段测验、错题复习、连续答对退出机制和模块总评均保存在本机浏览器。
- **默认本地安全**：网关仅监听 `127.0.0.1`，Redis Key 自动按会话隔离，命令和 JVM 探针均使用白名单。

## 当前课程

当前共有 24 个实验入口。

| 模块 | 课程范围 | 实验数 | 状态 |
| --- | --- | ---: | --- |
| Java 核心 | HashMap、ThreadPoolExecutor | 2 | 可用 |
| JVM 专题 | 字节码、类加载、运行时内存、对象分配、可达性、分代回收、收集器对比 | 7 | 基础完成，诊断建设中 |
| Redis 专题 | Cache-Aside、数据类型、TTL、缓存治理、并发正确性、持久化、高可用、诊断与综合演练 | 13 | 基础里程碑完成 |
| 数据与缓存 | MySQL 主键索引与全表扫描 | 1 | 可用 |
| 消息中间件 | Kafka 分区、消费组与位点提交 | 1 | 可用 |

### Redis 学习路径

1. 基础原理：Cache-Aside、常用数据类型、TTL、过期删除和内存淘汰。
2. 缓存治理：缓存穿透、热点 Key 击穿、缓存雪崩和流量保护。
3. 并发正确性：缓存一致性、原子命令、WATCH、Lua 和分布式锁。
4. 可靠性与运维：RDB/AOF、复制、Sentinel、Cluster、热 Key 和大 Key。
5. 综合实战：在商品缓存链路中组合分析穿透、击穿、雪崩和一致性风险。

13 课均提供受控的真实 Redis 命令步骤。缓存读写、数据类型、TTL 和原子扣减已经具备完整原理精讲；其余深化内容与多节点真实环境记录在 [Redis Backlog](docs/BACKLOG.md)。

### JVM 学习路径

1. 字节码与类加载：Class 文件、局部变量表、操作数栈、加载、链接、初始化和双亲委派。
2. 内存与对象：运行时数据区域、线程私有栈、共享堆、对象分配和 TLAB。
3. 垃圾回收：GC Roots、传递可达、不可达循环、分代假说、Young GC 和对象晋升。
4. 性能与诊断：Serial、Parallel、G1 的工作分布与选择边界。

每课都提供固定白名单的 `JDK 实机` 观测，覆盖 `javap`、ClassLoader、内存池、线程分配字节、弱引用和 GC MXBean。下一课是 GC 日志分析，随后建设 OOM/泄漏诊断与 JIT/逃逸分析。

详细进度见 [里程碑](docs/MILESTONES.md) 和 [内容路线图](docs/ROADMAP.md)。

## 快速开始

### 1. 只运行浏览器实验

环境要求：

- Node.js `22.12+`，推荐 Node.js 24 LTS
- npm

```bash
git clone https://github.com/GitHub-lcb/java-lab.git
cd java-lab
npm ci
npm run dev
```

打开终端输出的本机地址。开发服务器默认只监听 `127.0.0.1`。不启动后端也可以使用架构视图、模型实验、代码、日志、测验和学习中心。

### 2. 启用真实 Redis 与 JVM 实验

额外要求：

- JDK 8 或更高版本，`java` 和 `javac` 已加入 `PATH`
- Redis 7，可使用现有本机实例或 Docker Compose
- 使用 Docker Compose 时需要 Docker Desktop 或兼容的 Docker 环境

使用项目提供的临时 Redis：

```bash
docker compose -f server/docker-compose.yml up -d
npm run backend
```

另开一个终端启动前端：

```bash
npm run dev
```

默认服务地址：

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| 前端开发服务器 | 以 Vite 输出为准 | 仅监听本机 |
| Java 实验网关 | `http://127.0.0.1:8787` | Redis 白名单命令与 JVM 固定探针 |
| Redis | `127.0.0.1:6379` | Docker 配置不启用持久化 |

如果本机 `6379` 已有 Redis，不要重复启动 Compose；直接执行 `npm run backend`。自定义地址时可设置 `LAB_REDIS_HOST`、`LAB_REDIS_PORT` 和 `VITE_REDIS_LAB_API`，完整说明见 [网关文档](server/README.md) 与 [.env.example](.env.example)。

停止项目提供的 Redis：

```bash
docker compose -f server/docker-compose.yml down
```

## 常用命令

| 命令 | 用途 |
| --- | --- |
| `npm run dev` | 启动前端开发服务器 |
| `npm run backend` | 编译并启动本机 Java 实验网关 |
| `npm test` | 运行前端、模型、课程和运行时测试 |
| `npm run test:java` | 运行 Redis 策略与 JVM 探针 Java 自测，无需 Redis |
| `npm run build` | 构建生产静态资源到 `dist/` |
| `npm run preview` | 本机预览生产构建 |

CI 在每次 push 和 pull request 时执行依赖审计、81 项 Node 测试、生产构建和 Java 自测。

## 系统结构

```text
Browser
  |
  +-- React + XState + React Flow
  |     +-- 确定性教学模型
  |     +-- 课程、测验、错题与本机进度
  |
  +-- Java Gateway (可选，仅限 127.0.0.1)
        +-- 固定 JVM 探针
        +-- Redis 命令白名单 + 会话 Key 命名空间
              |
              +-- Local Redis 7
```

浏览器模型和真实实验是两条独立证据链：

- `架构视图` 使用确定性数据解释对象流转、并发关系和算法取舍。
- `真实实验` 与 `JDK 实机` 显示当前本机环境的真实响应，但只允许预定义操作。
- 学习结果保存在当前域名的 `localStorage`，不上传服务器，也不会跨浏览器同步。

## 目录结构

```text
java-lab/
|-- .github/workflows/       # GitHub Actions CI
|-- docs/                    # 里程碑、路线图与 Backlog
|-- server/                  # Java 8 网关、Redis RESP 客户端、JVM 探针
|-- src/
|   |-- *Catalog.js          # 课程、任务、验证题和官方资料
|   |-- *Models.js           # 确定性教学模型与逐步快照
|   |-- *DeepDives.js        # 原理精讲、证据表和业务迁移题
|   |-- RealRedisLab.jsx     # 真实 Redis 实验界面
|   |-- RealJvmLab.jsx       # JDK 实机观测界面
|   |-- LearningCenter.jsx   # 阶段测验、错题本和专题总评
|   `-- main.jsx             # 应用入口与实验工作区
|-- tests/                   # Node 回归测试
|-- CONTRIBUTING.md
|-- SECURITY.md
`-- package.json
```

新增课程必须同时补齐目录配置、可运行模型或真实实验、边界说明、官方参考和测试，不能只增加一个不可交互的入口。

## 安全边界

当前 Java 网关的设计目标是单机教学，不是公共 Redis 代理：

- HTTP 服务只监听 `127.0.0.1`，没有用户登录、TLS、持久身份或资源配额。
- Redis Key 自动改写为 `lab:<session>:<key>`，单会话可跟踪 Key 数量有限。
- 禁止任意 Lua、`CONFIG`、`KEYS`、`MONITOR`、清库命令和任意系统命令。
- JVM 探针不接受用户源码、类名、JVM 参数、文件路径或进程命令。
- 不要把端口 `8787` 暴露到公网，也不要连接生产 Redis 做课程实验。

公网真实实验必须先具备 HTTPS、身份认证、授权、限流、配额、审计、独立 Redis 资源和容器/进程隔离。具体要求见 [安全策略](SECURITY.md)。

## 构建与部署

只发布浏览器实验时：

```bash
npm ci
npm run build
```

将 `dist/` 部署到 GitHub Pages、Cloudflare Pages、Netlify、Vercel 或 Nginx。托管平台使用：

- 构建命令：`npm run build`
- 发布目录：`dist`

项目使用 URL hash 路由，例如 `/#redis` 和 `/#jvm-collectors`，静态主机不需要 history fallback。

当前建议先发布纯前端课程；真实实验继续在本机或受保护内网运行。公共真实实验环境的部署方向见 [内容路线图](docs/ROADMAP.md)。

## 内容与视觉约定

- 教学模型必须可重复，并明确列出省略的网络、调度、采样或故障因素。
- 真实实验必须显示真实返回值，不能用动画或固定文本冒充环境测量。
- Java 教学代码用于解释核心机制，不默认视为可直接复制到生产的实现。
- 界面不依赖第三方统计、登录服务、在线字体或外部图库。
- 后续需要位图时，仅使用生图工具生成的原创资产并在仓库中管理，不使用图库图片。

## 参与贡献

开始修改前请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。提交前至少执行：

```bash
npm ci
npm test
npm run test:java
npm run build
```

问题和功能建议可以通过 GitHub Issues 提交。安全问题不要公开附带密码、令牌、Redis dump 或生产地址，请按 [SECURITY.md](SECURITY.md) 说明处理。

## 技术栈与许可

- React 19、Vite 7、XState 5、React Flow 12
- Lucide React、LRUCache、d3-array
- IBM Plex Mono 通过 `@fontsource` 本地打包
- Java 8+ 本地网关，直接使用 RESP 协议连接 Redis

项目使用 [MIT License](LICENSE)。第三方依赖遵循各自许可证，React Flow 署名保留在实验架构视图中。
