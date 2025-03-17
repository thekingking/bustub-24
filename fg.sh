#!/bin/sh

work_path=$(dirname $0)
cd $work_path

if [ $# -lt 1 ]; then
    echo "Usage: $0 <executable> [executable_args...]"
    exit 1
fi

[ -f perf_with_stack.data ] && rm -f perf_with_stack.data
perf record -g -o perf_with_stack.data -- "$@"
perf script -i perf_with_stack.data | stackcollapse-perf.pl | flamegraph.pl > perf.svg
