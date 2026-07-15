#!/bin/sh
set -eu

size=${1:-3}
case "$size" in
  3|4|5|6|7|8|9) ;;
  *) printf 'cluster size must be between 3 and 9\n' >&2; exit 2 ;;
esac

metadata_rf=${METADATA_RF:-3}
discovery_dynamic=${DISCOVERY_DYNAMIC:-0}
federation_cell_size=${FEDERATION_CELL_SIZE:-0}

voter_list() {
  start=$1
  count=$2
  end=$((start + count - 1))
  result='['
  current=$start
  while [ "$current" -le "$end" ]; do
    [ "$current" -eq "$start" ] || result="$result, "
    result="$result$current"
    current=$((current + 1))
  done
  printf '%s]' "$result"
}

federation=false
root_voters='[]'
if [ "$federation_cell_size" -ne 0 ]; then
  case "$federation_cell_size" in
    3|4|5|6|7|8|9) ;;
    *) printf 'federation Cell size must be between 3 and 9\n' >&2; exit 2 ;;
  esac
  [ $((size % federation_cell_size)) -eq 0 ] || {
    printf 'cluster size must be divisible by federation Cell size\n' >&2
    exit 2
  }
  cell_count=$((size / federation_cell_size))
  [ "$cell_count" -ge 3 ] || {
    printf 'federation acceptance requires at least three Cells\n' >&2
    exit 2
  }
  [ "$metadata_rf" -le "$federation_cell_size" ] || {
    printf 'metadata RF cannot exceed the federation Cell size\n' >&2
    exit 2
  }
  federation=true
  second_root=$((federation_cell_size + 1))
  third_root=$((federation_cell_size * 2 + 1))
  root_voters="[1, $second_root, $third_root]"
fi
case "$metadata_rf" in
  3) initial_voters='[1, 2, 3]' ;;
  5)
    [ "$size" -ge 5 ] || { printf 'metadata RF=5 requires at least five nodes\n' >&2; exit 2; }
    initial_voters='[1, 2, 3, 4, 5]'
    ;;
  *) printf 'metadata RF must be 3 or 5\n' >&2; exit 2 ;;
esac

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output="$root/deploy/generated/$size"
mkdir -p "$output"

node=1
while [ "$node" -le "$size" ]; do
  config="$output/node-$node.toml"
  bootstrap=false
  cell_id=1
  cell_start=1
  node_initial_voters=$initial_voters
  if [ "$federation" = true ]; then
    cell_id=$(((node - 1) / federation_cell_size + 1))
    cell_start=$(((cell_id - 1) * federation_cell_size + 1))
    node_initial_voters=$(voter_list "$cell_start" "$metadata_rf")
    if [ "$node" -eq "$cell_start" ]; then bootstrap=true; fi
  elif [ "$node" -eq 1 ]; then
    bootstrap=true
  fi
  tcp_port=$((4150 + (node - 1) * 1000))
  http_port=$((4151 + (node - 1) * 1000))
  {
    printf 'log_format = "text"\n'
    printf '[node]\nid = %s\nbroadcast_address = "host.docker.internal"\n' "$node"
    printf '[network]\ntcp_address = "0.0.0.0:4150"\nhttp_address = "0.0.0.0:4151"\ninternal_address = "0.0.0.0:4250"\n'
    printf 'advertised_tcp_port = %s\nadvertised_http_port = %s\n' "$tcp_port" "$http_port"
    printf '[storage]\ndata_path = "/data"\nmax_segment_bytes = 104857600\nscrub_interval_seconds = 3600\n'
    printf 'entry_cache_bytes = 67108864\npayload_read_workers = 0\npayload_read_queue = 4096\n'
    printf 'dedup_max_entries = 1000000\ndedup_ttl_seconds = 600\n'
    printf 'disk_high_watermark_percent = 85\ndisk_low_watermark_percent = 75\nmin_free_bytes = 1073741824\n'
    printf '[queue]\ndefault_partitions = %s\nmax_partitions_per_topic = 1024\nmax_ack_gap = 65536\n' "$size"
    printf '[security.internal_tls]\ncertificate_file = "/certs/node-%s.pem"\nprivate_key_file = "/certs/node-%s.key"\n' "$node" "$node"
    printf 'client_ca_file = "/certs/ca.pem"\nroot_ca_file = "/certs/ca.pem"\nrequire_client_certificate = true\nrequired = true\n'
    printf '[cluster]\nenabled = true\nbootstrap = %s\nname = "rustqueue-%s-node"\n' "$bootstrap" "$size"
    printf 'initial_voters = %s\ndefault_replication_factor = 3\nmetadata_replication_factor = %s\n' "$node_initial_voters" "$metadata_rf"
    if [ "$federation" = true ]; then
      printf '[cluster.federation]\nenabled = true\ncell_id = %s\nroot_voters = %s\n' "$cell_id" "$root_voters"
      printf 'max_home_cells_per_topic = 128\nroute_cache_ms = 1000\nretry_after_ms = 1000\n'
      printf 'cell_min_nodes = %s\ncell_target_nodes = %s\ncell_max_nodes = %s\nrouters_per_cell = 3\n' "$federation_cell_size" "$federation_cell_size" "$federation_cell_size"
      printf 'catalog_state_split_bytes = 268435456\ncatalog_topic_split_count = 100000\n'
      printf 'catalog_ops_split_per_second = 5000\ncatalog_apply_p99_split_ms = 50\n'
    fi
    printf '[cluster.discovery]\nenabled = true\nlisten_address = "/ip4/0.0.0.0/tcp/4350"\n'
    printf 'seed_addresses = ["/dns4/rustqueue-%s/tcp/4350"]\nmdns = false\njoin_token_file = "/certs/discovery.token"\n' "$cell_start"
    printf 'announce_interval_seconds = 2\nmax_known_peers = 128\n'
    printf '[cluster.automation]\nenabled = true\npoll_interval_seconds = 2\nnode_stabilization_seconds = 2\n'
    printf 'node_down_grace_seconds = 10\ngroup_cooldown_seconds = 10\nmax_concurrent_migrations = 2\n'
    printf 'max_migrations_per_node = 1\nauto_replace_metadata = true\noperation_history_limit = 1000\n'
    printf '[cluster.shutdown]\ngrace_seconds = 30\nmaintenance_default_ttl_seconds = 1800\nmaintenance_max_ttl_seconds = 86400\n'
    peer=1
    while [ "$peer" -le "$size" ]; do
      include_peer=true
      if [ "$discovery_dynamic" = 1 ] && [ "$peer" -gt 3 ] && [ "$peer" -ne "$node" ]; then
        include_peer=false
      fi
      if [ "$include_peer" = false ]; then
        peer=$((peer + 1))
        continue
      fi
      peer_tcp=$((4150 + (peer - 1) * 1000))
      peer_http=$((4151 + (peer - 1) * 1000))
      printf '[cluster.nodes.%s]\n' "$peer"
      printf 'raft_address = "https://rustqueue-%s:4250"\n' "$peer"
      printf 'broadcast_address = "host.docker.internal"\n'
      printf 'tcp_port = %s\nhttp_port = %s\n' "$peer_tcp" "$peer_http"
      printf 'tls_server_name = "rustqueue-%s"\nfailure_domain = "zone-%s"\n' "$peer" "$peer"
      if [ "$federation" = true ]; then
        peer_cell=$(((peer - 1) / federation_cell_size + 1))
        printf 'cell_id = %s\nfederation_router = true\n' "$peer_cell"
      fi
      peer=$((peer + 1))
    done
  } > "$config"
  node=$((node + 1))
done

printf 'generated %s-node configs in %s\n' "$size" "$output"
