# App 与实验配置文档索引

本目录集中维护 app 相关说明和实验配置说明。

**当前多机实验以 RDMA 为准**：`RDMA多机实验.md`。

模块接口（按 `include/` 一层一头文件）见 [接口说明](../接口说明/README.md)。R2S 到数据面先读 [R2S到RDMA](../接口说明/通路/R2S到RDMA.md)。

## 文档导航

1. `RDMA多机实验.md`
   - Coin 符合-only + worker（synthetic / lsingle_replay）
   - 剖面：通路 / 正确性 / 回放 / soak / rate
   - preflight 与启动脚本

2. `测试说明.md`
   - 进程测试 vs 多机实验
   - `test_synthetic_singles`（CPU 配对）与 `test_rdma_orchestration` 覆盖范围

3. `状态机与调试.md`
   - Coin 编排 FSM 与 worker SourceState
   - 日志字段与可调参数对照
   - 用 pend / rdma / buf / lag 判瓶颈

4. `app部署与运行.md`
   - app 目录结构
   - 可执行程序说明
   - 配置文件分层
   - 编译与基础运行

5. `四机推荐配置.md`
   - 每台机器推荐硬件配置
   - 网卡/网线/交换机要求
   - DPDK 场景网络建议

6. `DPDK采集配置与使用.md`
   - DPDK 安装后检查项（工具链/网卡/大页）
   - 本项目 DPDK 采集接入要点
   - 联调步骤与常见排障

7. [接口说明](../接口说明/README.md)
   - R2S → packed singles → RDMA 槽
   - 与本目录实验文档分工：接口契约 vs 部署/握手/剖面

## 推荐阅读顺序

1. 先读 `app部署与运行.md`
2. 跨机 RDMA 读 `RDMA多机实验.md`
3. 进程测试与剖面判据读 `测试说明.md`
4. 看状态行、调缓冲/槽位读 `状态机与调试.md`
5. 实验计划硬件选型读 `四机推荐配置.md`
6. 准备启用 DPDK 采集时读 `DPDK采集配置与使用.md`
7. 模块契约读 [接口说明](../接口说明/README.md)（R2S → RDMA 从 [通路](../接口说明/通路/R2S到RDMA.md) 起）
