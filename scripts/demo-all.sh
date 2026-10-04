#!/usr/bin/env bash
set -Eeuo pipefail

base_url='http://localhost:8080'
with_failover=false
with_split_brain=false
failover_timeout_seconds=180

usage() {
  cat <<'EOF'
Usage: bash ./scripts/demo-all.sh [--base-url URL] [--with-failover] [--with-split-brain] [--failover-timeout-seconds N]

Requires: bash, curl, jq, Docker Compose. Failover and split-brain flags are
disruptive; use them only on the learning Compose cluster.
EOF
}

while (($#)); do
  case "$1" in
    --base-url)
      (($# >= 2)) || { usage >&2; exit 2; }
      base_url=$2
      shift 2
      ;;
    --with-failover) with_failover=true; shift ;;
    --with-split-brain) with_split_brain=true; shift ;;
    --failover-timeout-seconds)
      (($# >= 2)) || { usage >&2; exit 2; }
      [[ $2 =~ ^[1-9][0-9]*$ ]] || { printf 'Invalid failover timeout: %s\n' "$2" >&2; exit 2; }
      failover_timeout_seconds=$2
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

for command in docker curl jq; do
  command -v "$command" >/dev/null 2>&1 || {
    printf 'Missing dependency: %s\n' "$command" >&2
    exit 1
  }
done
docker compose version >/dev/null

cd "$(dirname "${BASH_SOURCE[0]}")/.."
base_url=${base_url%/}
run_id="$(date +%s)-$RANDOM"
player_a="demo-$run_id-a"
player_b="demo-$run_id-b"
redis_services=(redis-master redis-replica-1 redis-replica-2)
required_services=("${redis_services[@]}" sentinel-1 sentinel-2 sentinel-3 app)
tmp_dir=$(mktemp -d)
sse_pids=()
paused_services=()
stopped_master=''

cleanup() {
  local pid service
  for pid in "${sse_pids[@]}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  for service in "${paused_services[@]}"; do
    docker compose unpause "$service" || printf 'WARNING: could not unpause %s\n' "$service" >&2
  done
  if [[ -n $stopped_master ]]; then
    docker compose start "$stopped_master" || printf 'WARNING: could not start %s\n' "$stopped_master" >&2
  fi
  rm -f -- "$tmp_dir/sse-1" "$tmp_dir/sse-2" "$tmp_dir/sse-1.err" "$tmp_dir/sse-2.err"
  rmdir -- "$tmp_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

step() { printf '\n=== %s ===\n' "$1"; }
ok() { printf 'OK: %s\n' "$1"; }
fail() { printf 'FAIL: %s\n' "$1" >&2; exit 1; }

api() {
  local method=$1 path=$2
  if (($# == 3)); then
    curl -fsS --max-time 15 -X "$method" -H 'Content-Type: application/json' \
      --data "$3" "$base_url$path"
  else
    curl -fsS --max-time 15 -X "$method" "$base_url$path"
  fi
}

api_status() {
  local method=$1 path=$2
  if (($# == 3)); then
    curl -sS --max-time 15 -o /dev/null -w '%{http_code}' -X "$method" \
      -H 'Content-Type: application/json' --data "$3" "$base_url$path"
  else
    curl -sS --max-time 15 -o /dev/null -w '%{http_code}' -X "$method" "$base_url$path"
  fi
}

redis() {
  local service=$1
  shift
  docker compose exec -T "$service" redis-cli --raw "$@" | tr -d '\r'
}

redis_config() { redis "$1" CONFIG GET "$2" | tail -n 1; }
redis_value() { redis "$@" | head -n 1; }
master_ip() { redis sentinel-1 -p 26379 SENTINEL get-master-addr-by-name mymaster | head -n 1; }

wait_for_master_ip() {
  local deadline=$((SECONDS + 90)) address
  while ((SECONDS < deadline)); do
    address=$(master_ip 2>/dev/null || true)
    if [[ -n $address ]]; then printf '%s\n' "$address"; return 0; fi
    sleep 2
  done
  return 1
}

master_service() {
  local address=$1 service container_id ip
  for service in "${redis_services[@]}"; do
    container_id=$(docker compose ps -q "$service")
    [[ -n $container_id ]] || continue
    ip=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$container_id")
    if [[ $ip == "$address" ]]; then printf '%s\n' "$service"; return 0; fi
  done
  return 1
}

wait_for_api() {
  local deadline=$((SECONDS + 90)) status
  while ((SECONDS < deadline)); do
    status=$(api_status GET "/api/players/demo-$run_id-healthcheck" 2>/dev/null || true)
    [[ $status == 404 ]] && return 0
    sleep 2
  done
  return 1
}

wait_for_replica_count() {
  local service=$1 expected=$2 timeout=${3:-60}
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    if redis "$service" INFO replication | grep -qx "connected_slaves:$expected"; then return 0; fi
    sleep 2
  done
  return 1
}

wait_for_no_good_replicas() {
  local service=$1 deadline=$((SECONDS + 60))
  while ((SECONDS < deadline)); do
    if redis "$service" INFO replication | grep -qx 'min_slaves_good_slaves:0'; then return 0; fi
    sleep 2
  done
  return 1
}

restore_replica_runtime_config() {
  local service=$1 setting value result
  [[ $service != redis-master ]] || return 0
  while read -r setting value; do
    result=$(redis_value "$service" CONFIG SET "$setting" "$value")
    [[ $result == OK ]] || fail "Could not restore $setting on $service: $result"
  done <<'EOF'
appendonly yes
maxmemory 268435456
maxmemory-policy volatile-lru
EOF
  printf 'AOF and memory settings restored on %s\n' "$service"
}

start_sse_reader() {
  local file=$1 error_file=$2
  curl -sS -N -i --max-time 30 -H 'Accept: text/event-stream' \
    "$base_url/api/notifications/stream" >"$file" 2>"$error_file" &
  sse_pids+=("$!")
}

sse_payload() {
  sed -n 's/^data: //p' "$1" |
    jq -cs --arg player "$player_a" 'first(.[] | select(.playerId == $player and .kind == "level_changed")) // empty'
}

wait_for_sse_payload() {
  local file=$1 deadline=$((SECONDS + 20)) payload event_id
  while ((SECONDS < deadline)); do
    payload=$(sse_payload "$file" 2>/dev/null || true)
    if [[ -n $payload ]]; then
      event_id=$(jq -r '.id' <<<"$payload")
      if grep -Fq "id: $event_id" "$file"; then printf '%s\n' "$payload"; return 0; fi
    fi
    sleep 0.1
  done
  return 1
}

step '1. Docker, replication, Sentinel'
running=$(docker compose ps --status running --services)
if [[ -z $running ]]; then
  printf 'Cluster is stopped. Running docker compose up --build -d...\n'
  docker compose up --build --quiet-build --quiet-pull -d
  running=$(docker compose ps --status running --services)
fi
missing=()
for service in "${required_services[@]}"; do
  if ! grep -Fxq "$service" <<<"$running"; then missing+=("$service"); fi
done
if ((${#missing[@]})); then
  fail "Only part of the cluster is running: ${missing[*]}. Check docker compose ps -a and run docker compose up --build -d."
fi
for service in "${required_services[@]}"; do ok "$service is running"; done

master_address=$(wait_for_master_ip) || fail 'Sentinel did not find the master'
master=$(master_service "$master_address") || fail "Could not map master IP $master_address to a Compose service"
redis "$master" INFO replication | grep -qx 'role:master' || fail "Sentinel selected $master, but it is not a master"
ok "Sentinel selected $master ($master_address)"
wait_for_replica_count "$master" 2 || fail 'The master did not connect two replicas'
ok 'Two replicas are connected'
wait_for_api || fail 'The API did not become ready'
ok 'The API responds'
[[ $(redis_config "$master" appendonly) == yes ]] || fail 'AOF is disabled'
[[ $(redis_config "$master" appendfsync) == everysec ]] || fail 'appendfsync is not everysec'
[[ -n $(redis_config "$master" save) ]] || fail 'RDB snapshots are disabled'
[[ $(redis_config "$master" maxmemory-policy) == volatile-lru ]] || fail 'Wrong eviction policy'
(( $(redis_config "$master" maxmemory) > 0 )) || fail 'maxmemory is not configured'
(( $(redis_config "$master" min-replicas-to-write) >= 1 )) || fail 'min-replicas-to-write is not configured'
ok 'AOF/RDB, memory limit, eviction and write quorum are configured'

step '2. Profiles, pipeline, cache'
batch=$(jq -nc --arg run "$run_id" \
  '[range(1;21) | . as $n | {id:("demo-"+$run+"-batch-"+($n|tostring)), name:("Batch "+($n|tostring)), level:1, region:"eu"}]')
created=$(api POST '/api/players/batch' "$batch")
[[ $(jq 'length' <<<"$created") == 20 ]] || fail 'The batch request did not create 20 profiles'
ok '20 profiles were loaded through one batch request (pipeline)'
profile=$(api POST "/api/players/$player_a" '{"name":"Alice","level":2,"region":"eu"}')
[[ $(jq -r '.id' <<<"$profile") == "$player_a" ]] || fail 'Profile creation failed'
ok 'Profile created'
printf 'Profile: %s\n' "$profile"
[[ $(redis_value "$master" TYPE "player:$player_a") == hash ]] || fail 'Profile is not a Redis Hash'
ok 'Profile is stored as a Redis Hash'
api POST "/api/players/$player_b" '{"name":"Bob","level":1,"region":"us"}' >/dev/null
first=$(api GET "/api/players/$player_a")
ttl1=$(redis_value "$master" TTL "cache:player:$player_a")
sleep 2
second=$(api GET "/api/players/$player_a")
ttl2=$(redis_value "$master" TTL "cache:player:$player_a")
[[ $(jq -r '.id' <<<"$first") == "$(jq -r '.id' <<<"$second")" ]] || fail 'Repeated GET changed the profile'
((ttl1 > 0 && ttl2 > 0 && ttl2 < ttl1)) || fail "Cache TTL did not decrease: $ttl1 -> $ttl2"
ok "Cache filled; repeated GET did not reset TTL ($ttl1 -> $ttl2 s)"
updated=$(api POST "/api/players/$player_a" '{"name":"Alice Updated","level":2,"region":"eu"}')
invalidated_ttl=$(redis_value "$master" TTL "cache:player:$player_a")
[[ $(jq -r '.createdAt' <<<"$updated") == "$(jq -r '.createdAt' <<<"$profile")" ]] || fail 'createdAt changed'
((invalidated_ttl == -2)) || fail "Profile update did not invalidate cache: TTL=$invalidated_ttl"
ok 'Profile update preserved createdAt and invalidated cache'
api GET "/api/players/$player_a" >/dev/null

step '3. Logins, leaderboard, achievements'
login1=$(api POST "/api/players/$player_a/login")
login2=$(api POST "/api/players/$player_a/login")
login_ttl=$(redis_value "$master" TTL "logins:$player_a")
[[ $(jq -r '.count' <<<"$login1") == 1 && $(jq -r '.count' <<<"$login2") == 2 ]] || fail 'Login counter is incorrect'
((login_ttl > 0)) || fail 'Login counter has no TTL'
[[ $(redis_value "$master" TYPE "logins:$player_a") == string ]] || fail 'Login counter is not a Redis String'
ok "Two logins, counter 2, TTL $login_ttl s"
score_a=$(api POST '/api/leaderboard/score' "{\"playerId\":\"$player_a\",\"score\":120}")
score_b=$(api POST '/api/leaderboard/score' "{\"playerId\":\"$player_b\",\"score\":80}")
score_a2=$(api POST '/api/leaderboard/score' "{\"playerId\":\"$player_a\",\"score\":5}")
top=$(api GET '/api/leaderboard/top?limit=10')
rank=$(api GET "/api/leaderboard/rank/$player_a")
[[ $(redis_value "$master" TYPE tournament:main) == zset ]] || fail 'Leaderboard is not a Sorted Set'
jq -e '.score == 120' <<<"$score_a" >/dev/null || fail 'Initial score is incorrect'
jq -e '.score == 80' <<<"$score_b" >/dev/null || fail 'Second player score is incorrect'
jq -e '.score == 125' <<<"$score_a2" >/dev/null || fail 'Accumulated score is incorrect'
jq -e 'length > 0' <<<"$top" >/dev/null || fail 'Leaderboard top is empty'
jq -e '.rank >= 0' <<<"$rank" >/dev/null || fail 'Rank is missing'
ok "Sorted Set: score 120 + 5 = 125; rank $(jq -r '.rank' <<<"$rank")"
printf 'Top 3: %s\n' "$(jq -c '.[0:3]' <<<"$top")"
added=$(api POST "/api/players/$player_a/achievements" '{"name":"first_win"}')
duplicate=$(api POST "/api/players/$player_a/achievements" '{"name":"first_win"}')
api POST "/api/players/$player_b/achievements" '{"name":"first_win"}' >/dev/null
has=$(api GET "/api/players/$player_a/achievements/first_win")
common=$(api GET "/api/players/$player_a/achievements/common/$player_b")
[[ $(redis_value "$master" TYPE "achievements:$player_a") == set ]] || fail 'Achievements are not a Redis Set'
jq -e '.added == true' <<<"$added" >/dev/null || fail 'Achievement was not added'
jq -e '.added == false' <<<"$duplicate" >/dev/null || fail 'Duplicate achievement was added'
jq -e '.hasAchievement == true' <<<"$has" >/dev/null || fail 'Achievement lookup failed'
jq -e 'index("first_win") != null' <<<"$common" >/dev/null || fail 'Achievement intersection is incorrect'
ok 'Set: add, duplicate, membership and intersection'
printf 'Common achievements: %s\n' "$(jq -r 'join(", ")' <<<"$common")"

step '4. Redis Stream, consumer, SSE fan-out'
start_sse_reader "$tmp_dir/sse-1" "$tmp_dir/sse-1.err"
start_sse_reader "$tmp_dir/sse-2" "$tmp_dir/sse-2.err"
sleep 1
kill -0 "${sse_pids[0]}" 2>/dev/null || fail "First SSE client exited: $(cat "$tmp_dir/sse-1.err")"
kill -0 "${sse_pids[1]}" 2>/dev/null || fail "Second SSE client exited: $(cat "$tmp_dir/sse-2.err")"
changed=$(api PATCH "/api/players/$player_a/level" '{"delta":3}')
event1=$(wait_for_sse_payload "$tmp_dir/sse-1") || fail 'First SSE client did not receive the notification'
event2=$(wait_for_sse_payload "$tmp_dir/sse-2") || fail 'Second SSE client did not receive the notification'
event_id=$(jq -r '.id' <<<"$event1")
[[ $event_id == "$(jq -r '.id' <<<"$event2")" ]] || fail 'SSE clients received different Redis IDs'
grep -Fq 'event: level_changed' "$tmp_dir/sse-1" || fail 'First SSE event type is wrong'
grep -Fq 'event: level_changed' "$tmp_dir/sse-2" || fail 'Second SSE event type is wrong'
jq -e --arg player "$player_a" '.playerId == $player' <<<"$event1" >/dev/null || fail 'SSE player ID is wrong'
jq -e '.level == 5' <<<"$changed" >/dev/null || fail 'Player level did not change'
ok "Two SSE clients received Redis ID $event_id; level is 5"
printf 'SSE: event=level_changed id=%s data=%s\n' "$event_id" "$event1"
for pid in "${sse_pids[@]}"; do kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done
sse_pids=()
stream_length=$(redis_value "$master" XLEN notifications)
pending=-1
for ((attempt = 0; attempt < 20; attempt++)); do
  pending=$(redis_value "$master" XPENDING notifications notifications-group)
  ((pending == 0)) && break
  sleep 0.2
done
((stream_length > 0 && pending == 0)) || fail "Stream or XACK check failed: length=$stream_length pending=$pending"
ok "Redis Stream has $stream_length messages; consumer acknowledged them"

step '5. API errors'
missing=$(api_status GET "/api/players/demo-$run_id-missing")
invalid=$(api_status POST "/api/players/$player_a" '{"name":"","level":-1,"region":""}')
[[ $missing == 404 && $invalid == 400 ]] || fail "Unexpected API error statuses: $missing, $invalid"
ok 'Missing profile = 404; invalid input = 400'

if $with_failover; then
  step '6. Sentinel failover (stop master)'
  old_master=$master
  old_ip=$master_address
  stopped_master=$old_master
  docker compose stop "$old_master"
  deadline=$((SECONDS + failover_timeout_seconds))
  started=$SECONDS
  new_ip=''
  new_master=''
  while ((SECONDS < deadline)); do
    sleep 2
    candidate_ip=$(master_ip 2>/dev/null || true)
    if [[ -n $candidate_ip && $candidate_ip != "$old_ip" ]]; then
      candidate_master=$(master_service "$candidate_ip" 2>/dev/null || true)
      if [[ -n $candidate_master ]] && redis "$candidate_master" INFO replication 2>/dev/null | grep -qx 'role:master'; then
        new_ip=$candidate_ip
        new_master=$candidate_master
        break
      fi
    fi
    elapsed=$((SECONDS - started))
    if ((elapsed % 10 < 2)); then
      printf 'Waiting for failover: %s/%s s; Sentinel: %s\n' "$elapsed" "$failover_timeout_seconds" "${candidate_ip:-unavailable}"
    fi
  done
  [[ -n $new_master ]] || fail "Sentinel did not promote a new master in $failover_timeout_seconds seconds"
  ok "Sentinel switched master: $old_ip -> $new_ip ($new_master)"
  login_after_failover=''
  for ((attempt = 0; attempt < 10; attempt++)); do
    login_after_failover=$(api POST "/api/players/$player_a/login" 2>/dev/null || true)
    [[ -n $login_after_failover ]] && break
    sleep 2
  done
  [[ -n $login_after_failover ]] || fail 'API did not resume writes after failover'
  ok "API continues writing through $new_master"
  docker compose start "$old_master"
  stopped_master=''
  master=$(master_service "$(master_ip)") || fail 'Could not find master after recovery'
  wait_for_replica_count "$master" 2 "$failover_timeout_seconds" || fail 'Two replicas did not reconnect'
  ok 'Two replicas reconnected after recovery'
  restore_replica_runtime_config "$old_master"
fi

if $with_split_brain; then
  step '7. Split-brain protection (pause replicas)'
  master=$(master_service "$(master_ip)") || fail 'Could not find current master'
  running=$(docker compose ps --status running --services)
  replicas=()
  for service in "${redis_services[@]}"; do
    [[ $service == "$master" ]] || replicas+=("$service")
  done
  for service in "${replicas[@]}"; do
    grep -Fxq "$service" <<<"$running" || fail "$service is not running before pause"
    ok "$service is running before pause"
  done
  for service in "${replicas[@]}"; do
    paused_services+=("$service")
    docker compose pause "$service"
  done
  wait_for_no_good_replicas "$master" || fail 'Master still sees a good replica'
  ok 'Master sees no good replicas'
  write_result=$(redis "$master" SET "demo:split-brain:$run_id" 1 EX 30 2>&1 || true)
  [[ $write_result == *NOREPLICAS* ]] || fail "Master did not reject the write: $write_result"
  ok "Master rejected write: $write_result"
  for service in "${paused_services[@]}"; do docker compose unpause "$service"; done
  paused_services=()
  wait_for_replica_count "$master" 2 || fail 'Two replicas did not reconnect after unpause'
  ok 'Two replicas reconnected after unpause'
fi

printf '\nDemo completed. Player IDs: %s, %s. Data remains in Redis.\n' "$player_a" "$player_b"
