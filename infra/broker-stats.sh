#!/usr/bin/env bash
set -euo pipefail

# One exec, plain shell reads only, so the sampling itself adds negligible broker CPU.
# mem_bytes includes tmpfs pages (topic data), reported separately as data_dir_bytes.
docker exec kbench-kafka sh -c '
v1=/sys/fs/cgroup
if [ -r $v1/cpuacct/cpuacct.usage ]; then
  cpu=$(cat $v1/cpuacct/cpuacct.usage)
  mem=$(cat $v1/memory/memory.usage_in_bytes)
  peak=$(cat $v1/memory/memory.max_usage_in_bytes)
  ver=1
else
  cpu=$(( $(sed -n "s/^usage_usec //p" $v1/cpu.stat) * 1000 ))
  mem=$(cat $v1/memory.current)
  peak=$(cat $v1/memory.peak 2>/dev/null || echo null)
  ver=2
fi
data=$(df -P -k /var/lib/kafka/data 2>/dev/null | awk "NR==2 {print \$3 * 1024}")
printf "{\"cpu_ns\": %s, \"mem_bytes\": %s, \"mem_peak_bytes\": %s, \"data_dir_bytes\": %s, \"cgroup_version\": %s}\n" \
  "$cpu" "$mem" "$peak" "${data:-null}" "$ver"
'
