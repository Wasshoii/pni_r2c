# App Entrypoints

This directory contains deployable node applications built from the tested distributed pipeline.

## Programs

- `bin/app_acq_r2s_node`: acquisition sub-node (controlled by acquisition master), converts raw data to singles, and forwards singles to coin master.
- `bin/app_coin_master`: coin master node, receives singles stream and runs streaming coincidence; can optionally run acquisition master for task distribution/control.

## Config Files

- `app/config/examples/acq_r2s_node.example.json`
- `app/config/examples/coin_master.example.json`

## Build

```bash
make app-acq-r2s-node
make app-coin-master
```

## Run

```bash
./bin/app_coin_master --config app/config/examples/coin_master.example.json
./bin/app_acq_r2s_node --config app/config/examples/acq_r2s_node.example.json
```

## Dry Run

```bash
./bin/app_coin_master --config app/config/examples/coin_master.example.json --dry-run
./bin/app_acq_r2s_node --config app/config/examples/acq_r2s_node.example.json --dry-run
```
