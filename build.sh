#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

build_apps=false
build_tests=false
build_tools=false
debug_build=false
extra_cmake_args=()

print_usage() {
	cat <<EOF
Usage: $0 [--apps] [--tests] [--tools] [--all]

Options:
	--apps     Build app targets
	--tests    Build test targets
	--tools    Build tools targets
	--all      Build apps, tests, and tools (default)
	--debug    Build with debug symbols (CMAKE_BUILD_TYPE=Debug)
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
			--debug)
				debug_build=true
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

	if [[ "$debug_build" == true ]]; then
		extra_cmake_args=("-DCMAKE_BUILD_TYPE=Debug")
	fi

if [[ "$build_apps" == true ]]; then
	echo "[apps] Build apps (basic/cuda)"
		cmake --preset linux-release-apps-basic "${extra_cmake_args[@]}"
	cmake --build --preset build-apps-basic -j
		cmake --preset linux-release-apps-cuda "${extra_cmake_args[@]}"
	cmake --build --preset build-apps-cuda -j
fi

if [[ "$build_tests" == true ]]; then
	echo "[tests] Build tests"
		cmake --preset linux-release-tests-core "${extra_cmake_args[@]}"
	cmake --build --preset build-tests-core -j
		cmake --preset linux-release-tests-pni "${extra_cmake_args[@]}"
	cmake --build --preset build-tests-pni -j
		cmake --preset linux-release-tests-cuda "${extra_cmake_args[@]}"
	cmake --build --preset build-tests-cuda -j
fi

if [[ "$build_tools" == true ]]; then
	echo "[tools] Build tools"
		cmake --preset linux-release-tools "${extra_cmake_args[@]}"
	cmake --build --preset build-tools -j
fi

echo "Done."
