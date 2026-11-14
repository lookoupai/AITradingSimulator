#!/usr/bin/env bash
# AITradingSimulator 启动/停止/状态管理。
#
# 关键坑：必须用 .venv/bin/python 启动。系统 python 缺少 dotenv/flask 等
# 依赖；/proc/<pid>/exe 显示系统解释器只是 venv 软链的假象，不要被它骗了。
# 生产运行方式：screen 会话（默认 pc28，窗口 predictor），cwd 必须是仓库根。
#
# 用法：run_app.sh start|stop|restart|status|foreground
# 可用环境变量：AITRADING_SCREEN（screen 会话名，默认 pc28）、
#              AITRADING_WINDOW（窗口名，默认 predictor）
set -u

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSION="${AITRADING_SCREEN:-pc28}"
WINDOW="${AITRADING_WINDOW:-predictor}"
LOG="${APP_DIR}/pc28_predictor.log"
VENV_PY="${APP_DIR}/.venv/bin/python"

pids() {
  local p
  for p in $(pgrep -f "(^|/)python[0-9.]* app\.py" 2>/dev/null); do
    if [ "$(readlink "/proc/$p/cwd" 2>/dev/null)" = "$APP_DIR" ]; then
      echo "$p"
    fi
  done
}

preflight() {
  if [ ! -x "$VENV_PY" ]; then
    echo "缺少虚拟环境，创建中..."
    python3 -m venv "$APP_DIR/.venv"
    "$VENV_PY" -m pip install --quiet -r "$APP_DIR/requirements.txt"
  fi
  if ! "$VENV_PY" -c "import dotenv, flask" 2>/dev/null; then
    echo "错误：.venv 缺少依赖（dotenv/flask）。请在 $APP_DIR 执行：" >&2
    echo "  $VENV_PY -m pip install -r requirements.txt" >&2
    exit 1
  fi
}

cmd_start() {
  preflight
  local running
  running="$(pids)"
  if [ -n "$running" ]; then
    echo "已在运行（PID $running），跳过启动"
    return 0
  fi
  if screen -ls 2>/dev/null | grep -Eq "[0-9]+\.${SESSION}[[:space:]]"; then
    screen -S "$SESSION" -X screen -t "$WINDOW" bash -c "cd '$APP_DIR' && exec '$VENV_PY' app.py"
    echo "已在 screen 会话 $SESSION 新建窗口 $WINDOW 启动"
  else
    screen -dmS "$SESSION" bash -c "cd '$APP_DIR' && exec '$VENV_PY' app.py"
    echo "已创建 screen 会话 $SESSION 并启动"
  fi
  sleep 3
  cmd_status
}

cmd_stop() {
  local running
  running="$(pids)"
  if [ -z "$running" ]; then
    echo "没有运行中的实例"
    return 0
  fi
  echo "停止 $running ..."
  kill $running 2>/dev/null
  for _ in $(seq 1 30); do
    [ -z "$(pids)" ] && break
    sleep 0.5
  done
  if [ -n "$(pids)" ]; then
    echo "15 秒未退出，强制 KILL"
    kill -9 $(pids) 2>/dev/null
  fi
  echo "已停止"
}

cmd_restart() {
  cmd_stop
  cmd_start
}

cmd_status() {
  local running
  running="$(pids)"
  if [ -z "$running" ]; then
    echo "状态：未运行"
  else
    echo "状态：运行中（$(echo "$running" | wc -l) 个实例：$(echo "$running" | tr '\n' ' ')）"
    ps -o pid,etime,cmd -p $(echo "$running" | tr '\n' ',' | sed 's/,$//') 2>/dev/null | tail -n +2
  fi
  (cd "$APP_DIR" && "$VENV_PY" -c "import config; print('开奖保留: %s 天 / 预测保留: %s 天' % (config.PC28_DRAW_RETENTION_DAYS, config.PC28_PREDICTION_RETENTION_DAYS))" 2>/dev/null) || true
  [ -f "$LOG" ] && echo "最近日志：" && tail -3 "$LOG" | cut -c1-160
}

cmd_foreground() {
  preflight
  cd "$APP_DIR" && exec "$VENV_PY" app.py
}

case "${1:-status}" in
  start) cmd_start ;;
  stop) cmd_stop ;;
  restart) cmd_restart ;;
  status) cmd_status ;;
  foreground) cmd_foreground ;;
  *) echo "用法: $0 start|stop|restart|status|foreground"; exit 1 ;;
esac
