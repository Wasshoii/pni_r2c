#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

build_apps=false
build_tests=false
build_tools=false

print_usage() {
	cat <<EOF
Usage: $0 [--apps] [--tests] [--tools] [--all]

Options:
	--apps     Build app targets
	--tests    Build test targets
	--tools    Build tools targets
	--all      Build apps, tests, and tools (default)
EOF
}

if [[ $# -eq 0 ]]; then
	build_apps=true
	build_tests=true
	build_tools=true
else
	for arg in "$@"; do
		case "$arg" in
			--apps)
				build_apps=true
				;;
			--tests)
				build_tests=true
				;;
			--tools)
				build_tools=true
				;;
			--all)
				build_apps=true
				build_tests=true
				build_tools=true
				;;
			--help|-h)
				print_usage
				exit 0
				;;
			*)
				echo "Unknown option: $arg"
				print_usage
				exit 1
				;;
		esac
	done
fi

if [[ "$build_apps" == true ]]; then
	echo "[apps] Build apps (basic/cuda)"
	cmake --preset linux-release-apps-basic
	cmake --build --preset build-apps-basic -j
	cmake --preset linux-release-apps-cuda
	cmake --build --preset build-apps-cuda -j
fi

if [[ "$build_tests" == true ]]; then
	echo "[tests] Build tests"
	cmake --preset linux-release-tests-core
	cmake --build --preset build-tests-core -j
	cmake --preset linux-release-tests-pni
	cmake --build --preset build-tests-pni -j
	cmake --preset linux-release-tests-cuda
	cmake --build --preset build-tests-cuda -j
fi

if [[ "$build_tools" == true ]]; then
	echo "[tools] Build tools"
	cmake --preset linux-release-tools
	cmake --build --preset build-tools -j
fi

echo "Done."
