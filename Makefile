# Makefile (wrapper)
# The build system is CMake. This Makefile only forwards test commands to tests/.

.PHONY: all all-full test test-grpc test-streaming test-pni-r2c test-pni-coin \
	test-local-grpc-coin test-local-grpc-r2s test-acq-control-smoke \
	test-acq-control-init test-acq-datapath-udp test-acq-r2s-pipeline help

all all-full test test-grpc test-streaming test-pni-r2c test-pni-coin \
	test-local-grpc-coin test-local-grpc-r2s test-acq-control-smoke \
	test-acq-control-init test-acq-datapath-udp test-acq-r2s-pipeline help:
	$(MAKE) -C tests $@
