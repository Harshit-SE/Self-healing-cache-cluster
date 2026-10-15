#!/usr/bin/env bash
# Start the local cache-cluster demonstration with one command.
set -Eeuo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="$ROOT/.demo"
LOG_DIR="$STATE_DIR/logs"
PID_DIR="$STATE_DIR/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

CLIENT="$ROOT/build/cache_client"
SERVER="$ROOT/build/cache_server"
PYTHON="$ROOT/ai_service/.venv/bin/python"
UVICORN="$ROOT/ai_service/.venv/bin/uvicorn"
DOCKER=(docker)

say() { printf '[demo] %s\n' "$*"; }
fail() { printf '[demo] ERROR: %s\n' "$*" >&2; exit 1; }

pid_is_running() {
    local name="$1" pidfile="$PID_DIR/$1.pid" pid
    [[ -f "$pidfile" ]] || return 1
    read -r pid < "$pidfile" || return 1
    [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null
}

stop_pidfile() {
    local name="$1" pidfile="$PID_DIR/$1.pid" pid
    [[ -f "$pidfile" ]] || return 0
    read -r pid < "$pidfile" || true
    if [[ "${pid:-}" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        say "Stopped $name (pid $pid)"
    fi
    rm -f "$pidfile"
}

start_service() {
    local name="$1" url="$2"
    shift 2
    if curl --connect-timeout 1 --max-time 2 -fsS "$url" >/dev/null 2>&1; then
        say "$name is already responding; reusing it"
        return 0
    fi
    if pid_is_running "$name"; then
        say "$name process is already running; waiting for its endpoint"
    else
        say "Starting $name"
        nohup "$@" >"$LOG_DIR/$name.log" 2>&1 </dev/null &
        echo "$!" > "$PID_DIR/$name.pid"
    fi
    for _ in {1..40}; do
        if curl --connect-timeout 1 --max-time 2 -fsS "$url" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    say "$name did not become ready; recent log output:"
    tail -n 30 "$LOG_DIR/$name.log" 2>/dev/null || true
    fail "Could not start $name. See $LOG_DIR/$name.log"
}

ensure_docker() {
    command -v docker >/dev/null 2>&1 || fail "Docker is required to start Prometheus and Grafana."
    if docker info >/dev/null 2>&1; then
        DOCKER=(docker)
    elif command -v sudo >/dev/null 2>&1 && sudo -v && sudo docker info >/dev/null 2>&1; then
        DOCKER=(sudo docker)
    else
        fail "Docker daemon is unavailable. Start Docker and ensure this account can run Docker."
    fi
}

ensure_container() {
    local name="$1"
    shift
    if "${DOCKER[@]}" container inspect "$name" >/dev/null 2>&1; then
        local running
        running=$("${DOCKER[@]}" inspect -f '{{.State.Running}}' "$name")
        if [[ "$running" == "true" ]]; then
            say "$name container is already running; reusing it"
        else
            "${DOCKER[@]}" start "$name" >/dev/null
            touch "$PID_DIR/docker-$name.started"
            say "Started existing $name container"
        fi
    else
        "${DOCKER[@]}" run -d --name "$name" "$@" >/dev/null
        touch "$PID_DIR/docker-$name.started"
        say "Started $name container"
    fi
}

start_traffic() {
    "$CLIENT" --config "$ROOT/config/cluster.conf" \
        'v1 SET demo-hot-key - Repeated demo traffic' >/dev/null 2>&1 || true
    if pid_is_running traffic; then
        say "Demo traffic is already running"
        return
    fi
    say "Starting light background demo traffic to populate the live charts"
    nohup bash -c '
      client="$1" config="$2" count=0
      while :; do
        if (( count % 20 == 0 )); then
          "$client" --config "$config" "v1 SET demo-hot-key - Repeated demo traffic" >/dev/null 2>&1 || true
        else
          "$client" --config "$config" "v1 GET demo-hot-key" >/dev/null 2>&1 || true
        fi
        ((count += 1))
        sleep 0.05
      done
    ' demo-traffic "$CLIENT" "$ROOT/config/cluster.conf" \
      >"$LOG_DIR/traffic.log" 2>&1 </dev/null &
    echo "$!" > "$PID_DIR/traffic.pid"
}

start_collector() {
    if pid_is_running collector; then
        say "AI collector is already running"
        return
    fi
    say "Starting the AI collector"
    nohup "$PYTHON" "$ROOT/ai_service/collector.py" \
        --node http://127.0.0.1:17401 \
        --node http://127.0.0.1:17402 \
        --node http://127.0.0.1:17403 \
        --advisor http://127.0.0.1:8000 \
        >"$LOG_DIR/collector.log" 2>&1 </dev/null &
    echo "$!" > "$PID_DIR/collector.pid"
    sleep 1
    if ! pid_is_running collector; then
        tail -n 30 "$LOG_DIR/collector.log" 2>/dev/null || true
        fail "The AI collector stopped. See $LOG_DIR/collector.log"
    fi
}

start_demo() {
    command -v curl >/dev/null 2>&1 || fail "curl is required. Install it with: sudo apt install curl"
    command -v docker >/dev/null 2>&1 || fail "Docker is required for Prometheus and Grafana."
    if [[ ! -x "$SERVER" || ! -x "$CLIENT" ]]; then
        say "Building the C++ project (first run or missing binaries)"
        cmake -S "$ROOT" -B "$ROOT/build" -DCMAKE_BUILD_TYPE=Debug
        cmake --build "$ROOT/build" --parallel
    fi
    if [[ ! -x "$PYTHON" || ! -x "$UVICORN" ]]; then
        fail "Python advisor environment is missing. Create it with: python3 -m venv ai_service/.venv && ai_service/.venv/bin/pip install -r ai_service/requirements.txt"
    fi

    start_service cache-a http://127.0.0.1:17401/metrics \
        "$SERVER" --config "$ROOT/config/cluster.conf" --id cache-a --metrics-port 17401
    start_service cache-b http://127.0.0.1:17402/metrics \
        "$SERVER" --config "$ROOT/config/cluster.conf" --id cache-b --metrics-port 17402
    start_service cache-c http://127.0.0.1:17403/metrics \
        "$SERVER" --config "$ROOT/config/cluster.conf" --id cache-c --metrics-port 17403

    start_service advisor http://127.0.0.1:8000/health \
        "$UVICORN" main:app --app-dir "$ROOT/ai_service" --host 127.0.0.1 --port 8000

    if ! curl --connect-timeout 1 --max-time 2 -fsS http://127.0.0.1:9090/-/ready >/dev/null 2>&1 ||
       ! curl --connect-timeout 1 --max-time 2 -fsS http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
        ensure_docker
    fi
    if ! curl --connect-timeout 1 --max-time 2 -fsS http://127.0.0.1:9090/-/ready >/dev/null 2>&1; then
        ensure_container cache-prometheus --network host \
            -v "$ROOT/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
            prom/prometheus:latest \
            --config.file=/etc/prometheus/prometheus.yml \
            --web.listen-address=127.0.0.1:9090
    else
        say "Prometheus is already responding; reusing it"
    fi
    if ! curl --connect-timeout 1 --max-time 2 -fsS http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
        ensure_container cache-grafana --network host \
            -e GF_SERVER_HTTP_ADDR=127.0.0.1 \
            -v "$ROOT/monitoring/grafana/provisioning:/etc/grafana/provisioning:ro" \
            -v "$ROOT/monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro" \
            grafana/grafana:latest
    else
        say "Grafana is already responding; reusing it"
    fi

    for _ in {1..60}; do
        if curl --connect-timeout 1 --max-time 2 -fsS http://127.0.0.1:9090/-/ready >/dev/null 2>&1 &&
           curl --connect-timeout 1 --max-time 2 -fsS http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    curl --connect-timeout 1 --max-time 2 -fsS http://127.0.0.1:9090/-/ready >/dev/null 2>&1 ||
        fail "Prometheus is not ready. Check: ${DOCKER[*]} logs cache-prometheus"
    curl --connect-timeout 1 --max-time 2 -fsS http://127.0.0.1:3000/api/health >/dev/null 2>&1 ||
        fail "Grafana is not ready. Check: ${DOCKER[*]} logs cache-grafana"

    local cluster_status=""
    for _ in {1..30}; do
        cluster_status=$("$CLIENT" --config "$ROOT/config/cluster.conf" 'v1 STATUS' 2>/dev/null || true)
        if [[ "$cluster_status" == *"cache-a=ALIVE"* &&
              "$cluster_status" == *"cache-b=ALIVE"* &&
              "$cluster_status" == *"cache-c=ALIVE"* ]]; then
            break
        fi
        sleep 1
    done
    if [[ "$cluster_status" == *"cache-a=ALIVE"* &&
          "$cluster_status" == *"cache-b=ALIVE"* &&
          "$cluster_status" == *"cache-c=ALIVE"* ]]; then
        say "All three cache nodes report ALIVE"
    else
        say "Cluster is still converging; check STATUS and the cache logs before the demo"
        printf '%s\n' "$cluster_status"
    fi

    start_traffic
    start_collector
    say "Demo is ready. Open: http://localhost:3000/d/cache-cluster/self-healing-cache-cluster"
    say "Fresh Grafana login: admin / admin (Grafana may ask you to change it)."
    say "Prometheus targets: http://localhost:9090/targets"
    say "Logs: $LOG_DIR"
    say "Stop with: ./demo.sh stop"
}

stop_demo() {
    for name in traffic collector advisor cache-c cache-b cache-a; do
        stop_pidfile "$name"
    done
    if [[ -f "$PID_DIR/docker-cache-grafana.started" || -f "$PID_DIR/docker-cache-prometheus.started" ]]; then
        ensure_docker
    fi
    for name in cache-grafana cache-prometheus; do
        if [[ -f "$PID_DIR/docker-$name.started" ]]; then
            "${DOCKER[@]}" stop "$name" >/dev/null 2>&1 || true
            rm -f "$PID_DIR/docker-$name.started"
            say "Stopped $name container"
        fi
    done
    say "Stopped demo processes started by this script. Services that were already running were left alone."
}

show_status() {
    for endpoint in \
      http://127.0.0.1:17401/metrics \
      http://127.0.0.1:17402/metrics \
      http://127.0.0.1:17403/metrics \
      http://127.0.0.1:8000/health \
      http://127.0.0.1:9090/-/ready \
      http://127.0.0.1:3000/api/health; do
        if curl --connect-timeout 1 --max-time 2 -fsS "$endpoint" >/dev/null 2>&1; then
            printf 'UP   %s\n' "$endpoint"
        else
            printf 'DOWN %s\n' "$endpoint"
        fi
    done
}

case "${1:-start}" in
    start) start_demo ;;
    stop) stop_demo ;;
    status) show_status ;;
    *) printf 'Usage: %s [start|stop|status]\n' "$0" >&2; exit 2 ;;
esac
