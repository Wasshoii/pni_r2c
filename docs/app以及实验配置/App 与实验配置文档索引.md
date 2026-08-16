# App 与实验配置文档索引

本目录集中维护 app 相关说明和实验配置说明。

**当前多机实验以 RDMA 为准**：`RDMA多机实验.md`。

## 文档导航

1. `RDMA多机实验.md`
   - Coin 符合-only + worker（synthetic / lsingle_replay）
   - Register → OpenDataPlane → Start → Drain
   - preflight 与 2 机 / 3 机启动

2. `状态机与调试.md`
   - Coin 编排 FSM 与 worker SourceState
   - 日志字段与可调参数对照
   - 用 pend / rdma / buf / lag 判瓶颈

3. `app部署与运行.md`
   - app 目录结构
   - 可执行程序说明
   - 配置文件分层
   - 编译与基础运行

4. `四机推荐配置.md`
   - 每台机器推荐硬件配置
   - 网卡/网线/交换机要求
   - DPDK 场景网络建议

5. `DPDK采集配置与使用.md`
   - DPDK 安装后检查项（工具链/网卡/大页）
   - 本项目 DPDK 采集接入要点
   - 联调步骤与常见排障

## 推荐阅读顺序

1. 先读 `app部署与运行.md`
2. 跨机 RDMA 读 `RDMA多机实验.md`
3. 看状态行、调缓冲/槽位读 `状态机与调试.md`
4. 实验计划硬件选型读 `四机推荐配置.md`
5. 准备启用 DPDK 采集时读 `DPDK采集配置与使用.md`
