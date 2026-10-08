# swarm_topology_bridge

基于 ZeroMQ (Python) 的拓扑驱动型 ROS 桥接工具，运行时配置，无需重新编译。作为多 ROS 交互的通用通信组件部署在所有 ROS 节点上。

## 简介
- 一款轻量级的 ROS 桥接节点，利用 ZeroMQ 套接字在不同机器人之间传输指定的 ROS 话题。
- 专为集群场景设计，强调节点发现的灵活性和话题选择的可控性。
- 同时支持**实机多机部署**与**单机多 Master 隔离仿真**。
- 在 Topic 桥基础上，提供**通用 ROS Service 请求/响应代理**（ZMQ DEALER/ROUTER），支持跨 Master 的同步任务/安全请求，并保持对业务类型的透明（按配置动态解析消息类型）。

## 优势
- **去中心化**：不依赖统一的 ROS Master；各节点可按任意顺序启动并自动建立连接。
- **配置灵活**：在 YAML 中精确定义发送/接收的话题与要代理的 Service，避免冗余数据传输。
- **易于使用**：在一个配置文件中管理所有 IP、话题、Service 以及用于仿真的**端口偏移 (Port Offset)**。
- **命名空间隔离**：自动为接收到的远程话题添加来源无人机前缀（如 `/UAV6/pose`），有效防止集群中同名话题冲突。
- **通用性**：本包不参与任务业务，只做 ROS 原生序列化的 Topic 桥与 Service 代理；业务重试与幂等由上层业务包负责。

## 文件结构
```bash
└── swarm_topology_bridge
    ├── CMakeLists.txt
    ├── LICENSE
    ├── README.md                 # 中文主 README（唯一维护文档）
    ├── config
    │   ├── topology.yaml             # Group A 实机生产配置（GCS_A + UAV1..UAV16，port_offset=0）
    │   ├── topology_test.yaml        # 测试拓扑（UAV6..UAV10 + /test_srv），test.launch 专用
    │   ├── topology_group_a_sim.yaml # Group A 多 Master 仿真拓扑（GCS_A + UAV1..UAV16，唯一 owner）
    │   ├── topology_sim_swarm.yaml   # 多 Master 联合仿真配置
    │   └── topology_sim_single.yaml  # 单机回环测试配置
    ├── launch
    │   ├── test.launch               # 测试拓扑入口（topology_test.yaml + Service 代理测试）
    │   ├── bridge_group_a_real.launch # 实机单机 bridge 入口（生产 topology.yaml）
    │   ├── test_sim_swarm.launch     # 仿真多机集成测试启动文件
    │   ├── test_sim_single.launch    # 仿真单机回环测试启动文件
    │   └── topology_group_a_sim.launch # Group A 仿真拓扑 launch（bridge 入口）
    ├── package.xml
    └── scripts
        ├── bridge_node.py            # 核心桥接节点（Topic 桥 + Service 代理）
        ├── test_chatter.py           # 连通性测试脚本
        ├── test_swarm_chatter.py     # 多机连通性测试脚本（Topic 回环）
        └── test_service_node.py      # 通用 Service 代理测试脚本
```

## 安装
- 支持环境：Ubuntu 上的 ROS1 (如 Melodic/Noetic)；Python 3。
- 依赖项：`sudo apt install python3-zmq` 或 `pip3 install zmq`

```bash
# 创建工作空间
mkdir -p catkin_ws/src && cd catkin_ws/src
# 克隆仓库
git clone https://github.com/liang-hong/swarm_topology_bridge.git
# 编译
cd ..
catkin build swarm_topology_bridge
source devel/setup.bash
```

> **注意**：本项目推荐使用 `catkin_tools` 进行编译。使用 `catkin build` 可以实现更好的包隔离和并行编译。同时，本项目依然完全兼容传统的 `catkin_make` 编译方式，您可以根据现有工作空间的习惯进行选择。

## 环境变量（ROS 日志重定向）

为避免 ROS 运行日志散落在 `~/.ros/log`，本工作空间统一将运行日志重定向到 `.ros_home/`：

```bash
export ROS_HOME=/home/yjq/catkin_swarm6-2/.ros_home
export ROS_LOG_DIR=/home/yjq/catkin_swarm6-2/.ros_home/log
mkdir -p "$ROS_LOG_DIR"
```

手动启动前请先在终端执行以上命令，确保当前工作空间所有 ROS 运行日志均写入 `.ros_home/log`。

## 实机部署（implementation_plan_26091101）

- **一份生产配置**：`config/topology.yaml` 描述实机 GCS_A（`192.168.5.241`）与 UAV1–UAV16
  物理 IP；实机各机独立 IP，所有节点 `port_offset: 0`。端口冻结 `base_port: 4200`、
  `service_base_port: 14000`（已实机验证无冲突，防火墙按对应 TCP 范围放行）。
- **每机一个 bridge**：机载入口 `launch/bridge_group_a_real.launch`，按 hostname 判定本机身份
  （机载电脑 hostname 固定为 `UAVn`，地面站 hostname=`GCS_A`）。启动脚本只启动本机程序，
  不循环启动其他 UAV。
- **身份边界**：topology 与 bridge 只用执行层 `UAVn`；任务层 `Axx` 仅由 `tcp_to_ros` 处理。
- **仿真仍用 `topology_group_a_sim.yaml`**（GCS_A + UAV1–UAV16，`127.0.0.1` + `port_offset`；
  UAV16 独立 `port_offset: 1600`，仿真启动/任务测试不启动 UAV16，用于验证缺席机不阻塞原点 ready）。
- **测试隔离**：`launch/test.launch` 加载 `config/topology_test.yaml`（含 `/test_srv/*`），
  生产 `topology.yaml` 替换不影响该测试。

## 1. 配置说明

所有配置都在一个 YAML 中：`uavs`（IP/端口偏移）、`topics`（要桥接的话题）、`services`（要代理的 Service 与目标节点）、`topology`（谁能收谁的数据）、`base_port` / `service_base_port`。Topic 与 Service 共用同一套启动入口。

### 1.1 Topic 配置

```yaml
topics:
  - {name: mavros/state,                   type: mavros_msgs/State}
  - {name: leader_fix_origin,              type: sensor_msgs/NavSatFix}
  - {name: chatter,                        type: std_msgs/String}
max_freq: 50            # 全局默认限频; 单话题可用 max_freq 覆盖, null/0 表示不限频
base_port: 4000
```

- 每个话题可独立配置 `max_freq`（`null`/`0` 表示不限频），任务状态/控制类话题建议不限频以免丢消息；
- 启动窗口内的首条消息永不被限频逻辑丢弃；
- 接收端话题自动以来源名作为命名空间（如 `/UAV6/chatter`），避免同名冲突。

### 1.2 Service 配置

Service 代理使用建立在同一 ZMQ 静态拓扑之上的请求/响应通道，配置与 Topic 同构：

```yaml
services:
  - {name: /test_srv/UAV6, type: std_srvs/SetBool, target: UAV6}
  - {name: /test_srv/UAV7, type: std_srvs/SetBool, target: UAV7}
service_base_port: 14000
```

约定（每个节点加载同一份配置，按 `target` 判断角色）：

- 本机是 `target` 时：绑定 ZMQ ROUTER 端口，收到跨机请求后调用本机同名 ROS Service 并返回序列化响应；
- 本机不是 `target` 时：在本机 ROS Master 注册同名 Service 代理，收到上层调用后经 ZMQ 转发到目标节点；
- 每次请求携带 bridge 层唯一 request ID，支持并发请求、超时与迟到响应丢弃；
- bridge 只代理 ROS 序列化请求/响应，不理解任务业务，业务重试与幂等由上层负责。

## 1.3 Group A 多 Master 仿真拓扑（owner = 本包）

Group A（GCS_A + UAV1..UAV15）的多 Master 仿真拓扑配置**唯一**保存在
`config/topology_group_a_sim.yaml`，由本包的 `bridge_node` 加载（
implementation_plan_26082916 §7 将 owner 从 `tcp_to_ros` 迁移到本包）；
`tcp_to_ros` 只保留一个兼容 wrapper launch，不再保存重复拓扑内容。

每个 ROS Master 启动一次 bridge，通过 `uav_name` 区分角色：

```bash
# GCS_A 侧（地面站 Master 11310）
export ROS_MASTER_URI=http://localhost:11310
roslaunch swarm_topology_bridge topology_group_a_sim.launch uav_name:=GCS_A

# 机载 UAVn 侧（各自 Master 11311..11325）
export ROS_MASTER_URI=http://localhost:11311
roslaunch swarm_topology_bridge topology_group_a_sim.launch uav_name:=UAV1
```

- `uav_name`：本机角色名（`UAV1..UAV15` / `GCS_A`），bridge 按它匹配
  `uavs[].name` 决定本机 ZMQ 绑定端口与转发前缀；
- `config`：可选覆盖 topology 文件路径，默认
  `$(find swarm_topology_bridge)/config/topology_group_a_sim.yaml`；
- 配置加载：launch 内 `<rosparam file="$(arg config)" command="load"/>`
  将整份 YAML 加载为 `bridge_node` 私有参数，消息类型运行时由
  `roslib.message.get_message_class` 动态解析（如
  `geographic_msgs/GeoPointStamped`，依赖已在 package.xml/CMakeLists 声明）；
- 业务侧机载入口 `swarm_uav_executor/launch/uav_offboard_ego.launch` 与地面站
  兼容入口 `tcp_to_ros/launch/topology_group_a_sim.launch` 均 include 本 launch。

## 2. 单机回环测试

验证单个节点上 Topic 与 Service 都能"发出 → 经 ZMQ 收回"。`test_sim_single.launch` 会同时启动 `test_swarm_chatter.py`（Topic）与 `test_service_node.py`（Service）。

```bash
roslaunch swarm_topology_bridge test_sim_single.launch
```

### 关键预期输出（实测）

Topic 回环（节点日志，每 1 秒一次，`[TopicBridge]` SEND/RECV 成对出现）：

```text
[TopicBridge] SEND Hello from UAV_A! (count: 37)
[TopicBridge] RECV UAV_A -> us: Hello from UAV_A! (count: 37)
```

可另用 `rostopic echo` 确认（收到来自回环的 chatter）：

```bash
rostopic echo -n 1 /UAV_A/chatter
# data: "Hello from UAV_A! (count: 12)"
```

Service 回环（节点日志，每 2 秒一次，`[ServiceProxy]` SERVE/CALL 成对出现）：

```text
[ServiceProxy] SERVE /test_service_node request data=True
[ServiceProxy] CALL UAV_A -> /test_srv/UAV_A success=True msg=ok
```

可另用 `rosservice` 确认：

```bash
rosservice list | grep test_srv      # /test_srv/UAV_A
rosservice call /test_srv/UAV_A true # success: True  message: "ok"
```

## 3. 多 Master 联合仿真（单机模拟）

通过 `port_offset` 模拟多台独立的机载电脑环境，每个 ROS Master 一个终端。`test_sim_swarm.launch` 同时启动 Topic 回环（`test_swarm_chatter.py`）与 Service 代理测试（`test_service_node.py`）。

```bash
# 终端 1 (模拟 UAV6)
export ROS_MASTER_URI=http://localhost:11311
roslaunch swarm_topology_bridge test_sim_swarm.launch uav_name:=UAV6
# 终端 2 (模拟 UAV7)
export ROS_MASTER_URI=http://localhost:11312
roslaunch swarm_topology_bridge test_sim_swarm.launch uav_name:=UAV7
```

### 关键预期输出

Topic：在任一终端应看到本机发出的消息被其他 Master 转发回来后打印，例如 UAV6 侧：

```text
[TopicBridge] SEND Hello from UAV6! (count: 1)
[TopicBridge] RECV UAV7 -> us: Hello from UAV7! (count: 1)   # UAV7 Master 转发来
```

```bash
# 在 UAV6 Master 侧确认收到 UAV7 的消息
rostopic echo -n 1 /UAV7/chatter
# data: "Hello from UAV7! (count: 1)"
```

Service：任一终端应看到对拓扑邻居的跨 Master 调用成功：

```text
[ServiceProxy] CALL UAV6 -> /test_srv/UAV7 success=True msg=ok
[ServiceProxy] SERVE /test_service_node request data=True   # 收到的正是 UAV7 转来的请求
```

```bash
# 在 UAV6 Master 侧查看代理注册
rosservice list | grep test_srv   # 含 /test_srv/UAV6(本机) 与 /test_srv/UAV7(代理)
# 跨 Master 调用
rosservice call /test_srv/UAV7 true
# success: True  message: "ok"
```

## 4. 实机部署

修改 `config/topology.yaml` 以设置物理 IP、话题与需要代理的 Service；每台机载电脑分别执行（用 hostname 推断或 `uav_name` 显式指定本机身份）：

```bash
roslaunch swarm_topology_bridge test.launch uav_name:=UAV6
```

- 实机各节点 `port_offset` 全部为 0（同端口、不同物理 IP）；
- 上层业务节点在各自 ROS Master 上直接调用 `/test_srv/UAVn`（或业务 Service 名），bridge 自动完成跨 Master 转发；
- 业务 Topic 与 Service 的判读方式与第 2、3 节相同（`[TopicBridge]`/`[ServiceProxy]` 日志、`rostopic`/`rosservice` 辅助确认）。

## 5. 数据面观测与链路加固（2026-10-08 增）

两轮实飞（2026-09-22 三机 / 09-23 四机）coverage 失败时，bridge **对数据面零可观测**：
没有 per-topic 计数、没有最后到达时间、没有断链日志，`recv_loop` 连 try/except 都没有；
ZMQ 链路也没有任何心跳，IP 层半开即"永久静默、不重连"。逐层定位只能靠排除法。
本节参数由 `scripts/bridge_health.py` 实现（纯逻辑，可无 ROS 单测：`test/test_bridge_health.py`）。

### 5.1 健康日志（默认开）

按秒写 JSONL 到 `<health_log_dir>/<uav_name>_bridge_health.jsonl`（**不经 rosout**——任务窗口
rosout 本就无输出；文件随采集流程回取）：

| 行类型 | 内容 |
|---|---|
| `health_start` | 启动摘要：文件路径、采样率、KEY/停顿/监控话题、实际生效的链路选项 |
| `sample`（每秒） | 逐 KEY 话题：收发计数、限频丢弃数、`age_ms`、`max gap`、`>0.2/0.5/1.0/3.0 s` 间隔计数；接收侧再给"最旧源 `age_max_src`"与"陈旧源列表 `stale_srcs`"；另带 `load1`、进程 CPU 秒、`write_errors`、`stuck_sockets`（**套接字里有数据但没被取走** = 收包线程被饿死/卡住） |
| `matrix`（默认 10 s） | 逐 (源, 话题) 明细，定位到具体哪条链 |
| `stall_start` / `stall_end` | 停顿事件（含静默时长/持续时长），只对周期性话题（默认 odom/pose）判 |
| `link` | **连接级**事件（connected / disconnected / connect_retried…）——区分"数据没到"与"链路断了"的唯一直接信号 |
| `error` | 收包/反序列化/发布异常与 traceback（旧实现下这类异常会**静默杀死整个接收线程**） |

关键参数（都有默认值，仿真/测试入口不传也能用）：

| 参数 | 默认 | 说明 |
|---|---|---|
| `~health_log_dir` | `$HOME/catkin_swarm6-2/runtime_logs/health` | 空串/none/off = 关闭观测 |
| `~health_sample_hz` / `~health_matrix_period_s` | `1.0` / `10.0` | 矩阵是汇总行的 10 倍体积，默认降频 |
| `~health_key_topics` | odom/pose/state/intent/task_state | 逐话题汇总与矩阵覆盖范围 |
| `~stall_topics` | odom/pose | 只有高频周期性话题才判停顿（`trajectory_intent`/`uav_task_state` 事件驱动，天生静默） |
| `~stall_warn_s` | `1.5` | 停顿告警阈值（跟随环自身判据是 1.0 s） |
| `~monitor_topics` | odom | 开 ZMQ socket monitor 的话题（每源一个 monitor socket） |

### 5.2 链路加固（默认开）

`apply_link_options()` 在 `bind()/connect()` **之前**给所有 socket 装：

- `ZMQ_HEARTBEAT_IVL=1000ms` + `ZMQ_HEARTBEAT_TIMEOUT=10000ms`：ZMTP 心跳。超时取 10 s 是因为
  现场实测电台拥塞时会出现 1~3 s 空档（见 `config/topology.yaml` 注释），3 s 超时会把"半开兜底"
  变成高频拆链重建。**心跳只覆盖链路层**：对端进程活着但收包线程卡住时心跳照常，那种情况由
  `stuck_sockets` / `stall_*` 暴露。**TTL 不设**（静默话题不该被对端按"无数据"判超时）。
- `ZMQ_TCP_KEEPALIVE*`：OS 层半开检测（第二道）。
- `ZMQ_RECONNECT_IVL=200ms` / `MAX=1000ms`：断链退避（默认 100 ms 固定重试在 ~160 条链同时掉时
  会以 10 Hz/链 的 SYN 抖电台；上限取 1 s——跟随者 1.0 s 就判 `LEADER_LOST`，恢复不能更慢）。
- DEALER 另加 `ZMQ_SNDTIMEO=500ms`：DEALER 满 SNDHWM 时 `send_multipart` **会阻塞**（PUB 不会），
  否则服务路径会无声挂起。

### 5.3 分析入口

一次任务后，把各机与 GCS 的 `*_bridge_health.jsonl` 对齐时间轴，按
`(src=领机, topic=mavros/local_position/odom)` 看 `age_ms` / `gt1000` / `link` 事件即可定层：

| 现象 | 结论 |
|---|---|
| 发送侧 `local` 行 `rx` 停 | mavros → bridge 这一段（上游节点/本地订阅） |
| 发送侧 `rx` 走、`tx` 停 | bridge 发送路径（含限频丢弃，`drop` 字段可区分） |
| 接收侧 `rx` 停、发送侧 `tx` 走 | ZMQ/电台链路（配合 `link` 事件判断是否真的断开重连） |
| 接收侧 `rx` 走、执行器事件缺失 | 执行器内部（见 swarm_uav_executor 的 `*_executor_events.jsonl`） |

## 项目渊源
- 受 C++ 项目 [swarm_ros_bridge](https://github.com/shupx/swarm_ros_bridge) 启发。
- 本仓库为 Python 重构版本，针对配置灵活性、Service 代理和仿真适配进行了优化。

## 许可
- BSD-3-Clause