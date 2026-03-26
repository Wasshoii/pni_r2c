# Experiment Package

This package provides directly executable experiment configurations for correctness, balanced throughput, and stress tests.

## Build

```bash
make app-coin-master app-acq-r2s-node app-udp-replayer
```

## One-host quick runs

```bash
bash app/experiments/run_correctness_local.sh
bash app/experiments/run_balanced_local.sh
bash app/experiments/run_stress_local.sh
```

Logs are written under `app/experiments/logs/`.

## Three-machine deployment template

- Machine A: coin master node
- Machine B: acquisition + R2S nodes
- Machine C: UDP replay sender

### Network recommendation

- Data plane: dedicated 10/25GbE network, separate from control plane.
- Control plane: separate 1GbE VLAN or management switch.
- Use non-blocking switch and enable jumbo frames only if all links are configured consistently.

### Suggested addressing

- Coin master control IP: `10.0.0.10`
- Acquisition node host control IP: `10.0.0.20`
- Sender control IP: `10.0.0.30`
- Data plane IPs for detector simulation source can be independent, but must match `detectorSources` in coin config.

### Start order

1. Start coin master on Machine A.
2. Start 3 acquisition apps on Machine B.
3. Run UDP replayer on Machine C.

### Machine A command

```bash
./bin/app_coin_master --config app/config/experiments/coin_master_correctness.json
```

### Machine B commands

```bash
./bin/app_acq_r2s_node --config app/config/experiments/acq_node_0.json
./bin/app_acq_r2s_node --config app/config/experiments/acq_node_1.json
./bin/app_acq_r2s_node --config app/config/experiments/acq_node_2.json
```

### Machine C commands

```bash
./bin/app_udp_raw_replayer \
  --raw-path "Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch0_ch1_ch2_ch3.raw" \
  --source-ip 10.10.1.10 --destination-ip 10.10.9.20 \
  --source-port-base 17100 --destination-port-base 18100 \
  --channel-count 4 --max-segments 80 --inter-packet-us 2 --repeat 1
```

Adjust source/destination IP and ports according to actual NIC binding and config mapping.

## Correctness checks

- Compare coin logs: singles received, prompt/delay counts.
- Compare with offline baseline from existing tests for the same input slices.
- Verify no dropped segments in bridge stats and no node state error.

## Performance checks

- Increase replay concurrency and reduce inter-packet-us.
- Track CPU, memory, queue depth, and throughput.
- Record first failure point and recovery behavior.
