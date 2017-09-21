
echo "Cost for replication:"  awk "BEGIN {print `./cost_analysis.py $1` + `./comparing_latencies.py $1 results/format_multiGet`}"
echo "Cost for ECC:"  awk "BEGIN {print `./cost_analysis.py $2` + `./comparing_latencies.py $2 results/format_multiGet`}"
echo "Cost for Panda:" awk "BEGIN {print `./cost_analysis.py $3` + `./comparing_latencies.py $3 results/format_multiGet`}"


