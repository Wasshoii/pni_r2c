# Acquisition Control Draft (Phase 0)

## Goal

Provide a minimal but executable control-plane for acquisition nodes before the final distributed protocol is frozen.

This draft focuses on:

- node registration via bidirectional gRPC stream (`AcquisitionControlService.Connect`)
- per-node acquisition task delivery (`CMD_CONFIGURE`)
- coordinated start/stop (`CMD_START`, `CMD_STOP`)
- graceful shutdown (`CMD_SHUTDOWN`)

## Scope

- Control-plane protocol: `protos/acquisition.proto`
- Master service implementation: `include/distributed/AcquisitionMaster.hpp`
- Node state machine implementation: `src/grpcNode/acquisitionNode.hpp`

## Master API (current implementation)

- `WaitForConnectedNodes(count, timeoutMs)`
- `SendConfigureToNode(nodeId, task, message)` for manual per-node assignment
- `DistributeTasks()` for auto split from a global task
- `SendStart(plannedStartTimeMs, durationMs)`
- `SendStop(reason)`
- `SendShutdown(reason)`

## Command Semantics

`CMD_CONFIGURE`

- payload: `MasterCommand.task`
- required fields:
  - `algorithm_type` (`ALGORITHM_TYPE_SOCKET` or `ALGORITHM_TYPE_DPDK`)
  - `storage_unit_size`
  - `max_buffer_size`
  - `time_switch_buffer_ms`
  - `min_packet_size`
  - `channels[]`
- optional fields:
  - `session_name`
  - `reserved_storage_gib`
  - `max_file_size_mb`
  - `dpdk_options` (used when `algorithm_type=ALGORITHM_TYPE_DPDK`)
  - `detector_sources[]` + `destination_rule` + `channel_index_rule`
- behavior:
  - meaning of mapping rules:
    - `destination_rule`: how controller assigns destination `(ip,port)` for each detector source while expanding `channels[]`; this remains required to align with `acquisition.cpp` address format `source==destination`
    - `channel_index_rule`: how controller assigns `channel_index` for each generated channel
  - mapping precedence:
    - if `channels[]` is non-empty, use it directly
    - else if `detector_sources[]` is non-empty, controller expands it into `channels[]`
      - `ip_source`/`port_source` from each `detector_sources[i]`
      - `ip_destination` from `destination_rule.ip_destination`
      - `port_destination = destination_rule.port_destination_base + i * max(1, destination_rule.port_destination_stride)`
      - `channel_index = channel_index_rule.channel_index_base + i * max(1, channel_index_rule.channel_index_stride)`
      - default when `channel_index_rule` is omitted: `base=0`, `stride=1` (same sequential indexing style as `acquisition.cpp`)
  - node validates fields
  - node builds `NodeAcquisitionConfig` and `AcquisitionInfo`
  - node creates acquisition runtime but does not start yet
  - node transitions to `STATE_CONFIGURED`

`CMD_START`

- payload: `MasterCommand.start_control`
- fields:
  - `planned_start_time_ms`: planned epoch milliseconds (optional, default immediate)
  - `duration_ms`: planned run duration in milliseconds (optional, default until stop)
- compatibility:
  - node still accepts legacy `MasterCommand.message` key/value format as fallback
- behavior:
  - node waits until `planned_start_time_ms` if present
  - node starts acquisition runtime
  - node transitions to `STATE_RUNNING`
  - optional duration timer auto-stops node to `STATE_CONFIGURED`

`CMD_STOP`

- payload: `MasterCommand.stop_control`
- fields:
  - `reason` (optional)
- compatibility:
  - node still accepts legacy free text in `MasterCommand.message` as fallback
- behavior:
  - node stops acquisition runtime
  - node keeps configuration
  - node transitions to `STATE_CONFIGURED`

`CMD_SHUTDOWN`

- payload: `MasterCommand.shutdown_control`
- fields:
  - `reason` (optional)
- compatibility:
  - node still accepts legacy free text in `MasterCommand.message` as fallback
- behavior:
  - node stops acquisition runtime
  - node closes stream and exits run loop
  - node transitions to `STATE_IDLE` before exit

## Node State Machine

States map to `NodeState` in `acquisition.proto`.

- `STATE_IDLE`
  - initial state after stream established
  - transitions:
    - `CMD_CONFIGURE` -> `STATE_CONFIGURED`

- `STATE_CONFIGURED`
  - acquisition config is ready, runtime is instantiated
  - transitions:
    - `CMD_START` -> `STATE_RUNNING`
    - `CMD_CONFIGURE` -> `STATE_CONFIGURED` (reconfigure)
    - `CMD_SHUTDOWN` -> exit

- `STATE_RUNNING`
  - acquisition loop and monitor loop are active
  - transitions:
    - `CMD_STOP` -> `STATE_CONFIGURED`
    - timer expiry (if `duration_ms`) -> `STATE_CONFIGURED`
    - runtime failure -> `STATE_ERROR`

- `STATE_ERROR`
  - command parsing/validation/runtime failure
  - transitions:
    - `CMD_CONFIGURE` -> `STATE_CONFIGURED` (if valid)
    - `CMD_SHUTDOWN` -> exit

## Status Reporting

Node periodically writes `NodeStatus` through the same gRPC stream.

Included fields:

- identity (`NodeInfo.node_id`, host, ip, cpu, memory)
- lifecycle state (`state`)
- runtime metrics (`current_speed_mpps`, `current_bandwidth_mbps`)
- buffer usage (`buffer_used`, `buffer_volume`, `buffer_usage_percent`)
- packet counters (`total_rx_packets`, `total_rx_bytes`, `error_packets`)
- diagnostic text (`error_message`)

## Notes for Compatibility

Control commands now use explicit protobuf fields (`start_control`, `stop_control`, `shutdown_control`) as the primary path. Legacy `message` parsing is intentionally retained for backward compatibility during transition.
