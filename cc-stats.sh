#!/bin/bash

# Claude Code 用量统计工具
# 使用 OTEL Collector 收集数据，DuckDB 分析

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
CONFIG_FILE="${SCRIPT_DIR}/otel-collector-config.yaml"
CONTAINER_NAME="cc-stats-otel-collector"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 启动 OTEL Collector
start_collector() {
    log_info "启动 OTEL Collector..."

    # 创建数据目录
    mkdir -p "${DATA_DIR}"

    cd "${SCRIPT_DIR}"
    docker compose up -d

    log_info "OTEL Collector 已启动，监听 http://localhost:4318"
}

# 停止 OTEL Collector
stop_collector() {
    log_info "停止 OTEL Collector..."

    cd "${SCRIPT_DIR}"
    docker compose down

    log_info "OTEL Collector 已停止"
}

# 查看 Collector 状态
status_collector() {
    cd "${SCRIPT_DIR}"
    docker compose ps
}

# 查看统计数据
show_stats() {
    log_info "查询 Claude Code 用量统计..."

    METRICS_FILE="${DATA_DIR}/claude-metrics.jsonl"

    if [[ ! -f "${METRICS_FILE}" ]]; then
        log_error "数据文件不存在: ${METRICS_FILE}"
        log_info "请确保 OTEL Collector 正在运行，并且 Claude Code 已配置 OTEL 导出"
        exit 1
    fi

    duckdb -c "SET VARIABLE metrics_path = '${METRICS_FILE}';" -c ".read '${SCRIPT_DIR}/stats.sql'"
}

# 按时间范围查询
query_range() {
    local start_time="${1:-}"
    local end_time="${2:-}"

    METRICS_FILE="${DATA_DIR}/claude-metrics.jsonl"

    if [[ ! -f "${METRICS_FILE}" ]]; then
        log_error "数据文件不存在: ${METRICS_FILE}"
        exit 1
    fi

    if [[ -z "${start_time}" ]]; then
        log_info "用法: $0 range <start_time> [end_time]"
        log_info "示例: $0 range '2026-01-01' '2026-01-31'"
        log_info "      $0 range '2026-01-01 09:00:00'"
        exit 1
    fi

    log_info "查询时间范围: ${start_time} 至 ${end_time:-现在}"

    duckdb -markdown -c "
INSTALL otlp FROM community;
LOAD otlp;
SET TimeZone = 'Asia/Shanghai';

SET VARIABLE metrics_path = '${METRICS_FILE}';
SET VARIABLE start_ts = TIMESTAMP '${start_time}';
SET VARIABLE end_ts = COALESCE(TRY_CAST('${end_time}' AS TIMESTAMP), NOW());

SELECT '=== 查询时间范围 ===' as info;
SELECT getvariable('start_ts') as start_time, getvariable('end_ts') as end_time;

SELECT '=== Token 用量 ===' as info;
SELECT
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens,
    SUM(CASE WHEN Attributes['type'] = 'cacheRead' THEN Value ELSE 0 END) as cache_read_tokens
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage'
  AND (Timestamp::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai' BETWEEN getvariable('start_ts') AND getvariable('end_ts');

SELECT '=== 按天明细 ===' as info;
SELECT
    DATE_TRUNC('day', (Timestamp::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai')::DATE as date,
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage'
  AND (Timestamp::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai' BETWEEN getvariable('start_ts') AND getvariable('end_ts')
GROUP BY 1
ORDER BY 1;
"
}

# 实时监控
live_monitor() {
    local minutes="${1:-5}"

    METRICS_FILE="${DATA_DIR}/claude-metrics.jsonl"

    if [[ ! -f "${METRICS_FILE}" ]]; then
        log_error "数据文件不存在: ${METRICS_FILE}"
        exit 1
    fi

    log_info "实时监控 (最近 ${minutes} 分钟)..."

    duckdb -markdown -c "
INSTALL otlp FROM community;
LOAD otlp;
SET TimeZone = 'Asia/Shanghai';

SET VARIABLE metrics_path = '${METRICS_FILE}';

SELECT '=== 最近 ${minutes} 分钟活动 ===' as info;
SELECT
    DATE_TRUNC('minute', (Timestamp::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai') as minute,
    MetricName,
    ROUND(SUM(Value), 2) as value
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE Timestamp::TIMESTAMP > NOW() - INTERVAL '${minutes} minutes'
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

SELECT '=== Token 汇总 ===' as info;
SELECT
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage'
  AND Timestamp::TIMESTAMP > NOW() - INTERVAL '${minutes} minutes';
"
}

# 按小时统计
hourly_stats() {
    local hours="${1:-24}"

    METRICS_FILE="${DATA_DIR}/claude-metrics.jsonl"

    if [[ ! -f "${METRICS_FILE}" ]]; then
        log_error "数据文件不存在: ${METRICS_FILE}"
        exit 1
    fi

    log_info "按小时统计 (最近 ${hours} 小时)..."

    duckdb -markdown -c "
INSTALL otlp FROM community;
LOAD otlp;
SET TimeZone = 'Asia/Shanghai';

SET VARIABLE metrics_path = '${METRICS_FILE}';

SELECT
    DATE_TRUNC('hour', (Timestamp::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai') as hour,
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens,
    COUNT(DISTINCT Attributes['session.id']) as sessions
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage'
  AND Timestamp::TIMESTAMP > NOW() - INTERVAL '${hours} hours'
GROUP BY 1
ORDER BY 1 DESC;
"
}

# 会话详情
session_stats() {
    METRICS_FILE="${DATA_DIR}/claude-metrics.jsonl"

    if [[ ! -f "${METRICS_FILE}" ]]; then
        log_error "数据文件不存在: ${METRICS_FILE}"
        exit 1
    fi

    log_info "会话详情..."

    duckdb -markdown -c "
INSTALL otlp FROM community;
LOAD otlp;
SET TimeZone = 'Asia/Shanghai';

SET VARIABLE metrics_path = '${METRICS_FILE}';

SELECT
    LEFT(Attributes['session.id'], 8) as session_id,
    (MIN(Timestamp::TIMESTAMP) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai' as start_time,
    (MAX(Timestamp::TIMESTAMP) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai' as last_activity,
    ROUND(EXTRACT(EPOCH FROM (MAX(Timestamp::TIMESTAMP) - MIN(Timestamp::TIMESTAMP))) / 60, 1) as duration_min,
    COALESCE(Attributes['model'], 'unknown') as model,
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage'
GROUP BY Attributes['session.id'], 5
ORDER BY 2 DESC
LIMIT 20;
"
}

# 查看原始数据
show_raw() {
    log_info "查看原始数据..."

    METRICS_FILE="${DATA_DIR}/claude-metrics.jsonl"

    if [[ -f "${METRICS_FILE}" ]]; then
        log_info "Metrics 文件 (最后 10 行):"
        tail -10 "${METRICS_FILE}" | jq '.'
    else
        log_warn "Metrics 文件不存在"
    fi
}

# 清理数据
clean_data() {
    log_warn "清理所有数据..."
    read -p "确定要清理所有数据吗? (y/N): " confirm
    if [[ "${confirm}" == "y" || "${confirm}" == "Y" ]]; then
        rm -rf "${DATA_DIR}"/*.jsonl
        log_info "数据已清理"
    else
        log_info "取消清理"
    fi
}

# 显示 Claude Code 配置指南
show_config_guide() {
    cat << 'EOF'
=== Claude Code OTEL 配置指南 ===

在 ~/.claude/settings.json 中添加以下配置:

{
  "telemetry": {
    "otel_endpoint": "http://localhost:4318"
  }
}

或者设置环境变量:

export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"

配置完成后，重启 Claude Code 即可开始收集数据。
EOF
}

# TUI 仪表盘
show_dashboard() {
    local refresh="${1:-5}"

    METRICS_FILE="${DATA_DIR}/claude-metrics.jsonl"

    if [[ ! -f "${METRICS_FILE}" ]]; then
        log_error "数据文件不存在: ${METRICS_FILE}"
        exit 1
    fi

    # 创建临时脚本用于 watch
    local temp_script=$(mktemp)
    cat > "$temp_script" << 'DASHBOARD_SCRIPT'
#!/bin/bash
METRICS_FILE="$1"

# 颜色
BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║              Claude Code 用量监控仪表盘                          ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# 获取统计数据
STATS=$(duckdb -json -c "
INSTALL otlp FROM community;
LOAD otlp;
SET TimeZone = 'Asia/Shanghai';

SELECT
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as total_input,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as total_output,
    COUNT(DISTINCT Attributes['session.id']) as total_sessions
FROM read_otlp_metrics('${METRICS_FILE}')
WHERE MetricName = 'claude_code.token.usage';
" 2>/dev/null)

TOTAL_INPUT=$(echo "$STATS" | jq -r '.[0].total_input // 0')
TOTAL_OUTPUT=$(echo "$STATS" | jq -r '.[0].total_output // 0')
TOTAL_SESSIONS=$(echo "$STATS" | jq -r '.[0].total_sessions // 0')

# 今日统计
TODAY_STATS=$(duckdb -json -c "
INSTALL otlp FROM community;
LOAD otlp;
SET TimeZone = 'Asia/Shanghai';

SELECT
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as today_input,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as today_output
FROM read_otlp_metrics('${METRICS_FILE}')
WHERE MetricName = 'claude_code.token.usage'
  AND DATE_TRUNC('day', (Timestamp::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai')::DATE = CURRENT_DATE;
" 2>/dev/null)

TODAY_INPUT=$(echo "$TODAY_STATS" | jq -r '.[0].today_input // 0')
TODAY_OUTPUT=$(echo "$TODAY_STATS" | jq -r '.[0].today_output // 0')

# 格式化数字
format_num() {
    local num=$1
    if [[ $(echo "$num > 1000000" | bc -l) -eq 1 ]]; then
        printf "%.2fM" $(echo "$num / 1000000" | bc -l)
    elif [[ $(echo "$num > 1000" | bc -l) -eq 1 ]]; then
        printf "%.1fK" $(echo "$num / 1000" | bc -l)
    else
        printf "%.0f" $num
    fi
}

echo -e "${CYAN}┌─────────────────────────────────────────────────────────────────┐${NC}"
echo -e "${CYAN}│${NC} ${BOLD}总计统计${NC}                                                        ${CYAN}│${NC}"
echo -e "${CYAN}├─────────────────────────────────────────────────────────────────┤${NC}"
printf "${CYAN}│${NC}  Input Tokens:  ${GREEN}%-15s${NC} Output Tokens: ${GREEN}%-15s${NC}  ${CYAN}│${NC}\n" "$(format_num $TOTAL_INPUT)" "$(format_num $TOTAL_OUTPUT)"
printf "${CYAN}│${NC}  会话总数:      ${YELLOW}%-15s${NC}                               ${CYAN}│${NC}\n" "$TOTAL_SESSIONS"
echo -e "${CYAN}└─────────────────────────────────────────────────────────────────┘${NC}"
echo ""

echo -e "${CYAN}┌─────────────────────────────────────────────────────────────────┐${NC}"
echo -e "${CYAN}│${NC} ${BOLD}今日统计${NC}                                                        ${CYAN}│${NC}"
echo -e "${CYAN}├─────────────────────────────────────────────────────────────────┤${NC}"
printf "${CYAN}│${NC}  Input Tokens:  ${GREEN}%-15s${NC} Output Tokens: ${GREEN}%-15s${NC}  ${CYAN}│${NC}\n" "$(format_num $TODAY_INPUT)" "$(format_num $TODAY_OUTPUT)"
echo -e "${CYAN}└─────────────────────────────────────────────────────────────────┘${NC}"
echo ""

# 最近活动
echo -e "${CYAN}┌─────────────────────────────────────────────────────────────────┐${NC}"
echo -e "${CYAN}│${NC} ${BOLD}最近会话${NC}                                                        ${CYAN}│${NC}"
echo -e "${CYAN}├─────────────────────────────────────────────────────────────────┤${NC}"

# 使用 list 模式获取数据，在 bash 中格式化
duckdb -list -noheader -c "
INSTALL otlp FROM community;
LOAD otlp;
SET TimeZone = 'Asia/Shanghai';

SELECT
    LEFT(Attributes['session.id'], 8) as sid,
    strftime((MIN(Timestamp::TIMESTAMP) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai', '%H:%M') as start_time,
    strftime((MAX(Timestamp::TIMESTAMP) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai', '%H:%M') as end_time,
    CASE
        WHEN SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) > 1000000
        THEN printf('%.1fM', SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) / 1000000.0)
        WHEN SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) > 1000
        THEN printf('%.0fK', SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) / 1000.0)
        ELSE printf('%.0f', SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END))
    END as input_tokens,
    CASE
        WHEN SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) > 1000000
        THEN printf('%.1fM', SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) / 1000000.0)
        WHEN SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) > 1000
        THEN printf('%.0fK', SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) / 1000.0)
        ELSE printf('%.0f', SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END))
    END as output_tokens
FROM read_otlp_metrics('${METRICS_FILE}')
WHERE MetricName = 'claude_code.token.usage'
GROUP BY Attributes['session.id']
ORDER BY MIN(Timestamp::TIMESTAMP) DESC
LIMIT 5;
" 2>/dev/null | while IFS='|' read -r sid start end input output; do
    printf "${CYAN}│${NC}  %-8s  %s ~ %s  %8s in  %6s out ${CYAN}│${NC}\n" "$sid" "$start" "$end" "$input" "$output"
done

echo -e "${CYAN}└─────────────────────────────────────────────────────────────────┘${NC}"
echo ""
REFRESH_TIME="$2"
echo -e "按 ${BOLD}Ctrl+C${NC} 退出 | 每 ${REFRESH_TIME}s 刷新"
DASHBOARD_SCRIPT

    chmod +x "$temp_script"

    log_info "启动仪表盘 (每 ${refresh} 秒刷新, Ctrl+C 退出)..."

    trap 'rm -f "$temp_script"; exit 0' INT

    while true; do
        clear
        "$temp_script" "$METRICS_FILE" "$refresh"
        sleep "$refresh"
    done

    rm -f "$temp_script"
}

# 帮助信息
show_help() {
    cat << EOF
Claude Code 用量统计工具

用法: $0 <command> [args]

命令:
    start           启动 OTEL Collector
    stop            停止 OTEL Collector
    status          查看 Collector 状态
    stats           查看完整用量统计
    dashboard [N]   实时仪表盘 (默认每 5 秒刷新)
    hourly [N]      按小时统计 (默认最近 24 小时)
    sessions        查看会话详情
    live [N]        实时监控 (默认最近 5 分钟)
    range <start> [end]  查询指定时间范围
    import [N]      导入最近 N 天历史数据 (默认 30 天)
    import --force [N]   强制重新导入历史数据
    raw             查看原始数据
    clean           清理数据
    config          显示 Claude Code 配置指南
    help            显示帮助信息

示例:
    $0 start                          # 启动收集器
    $0 stats                          # 查看完整统计
    $0 dashboard                      # 实时仪表盘
    $0 dashboard 2                    # 每 2 秒刷新
    $0 hourly                         # 最近 24 小时按小时统计
    $0 hourly 48                      # 最近 48 小时按小时统计
    $0 sessions                       # 查看会话列表
    $0 live                           # 最近 5 分钟实时数据
    $0 live 10                        # 最近 10 分钟实时数据
    $0 range '2026-01-01'             # 从 2026-01-01 至今
    $0 range '2026-01-01' '2026-01-31'  # 指定时间范围
    $0 import                         # 导入最近 30 天历史
    $0 import 365                     # 导入最近一年历史
    $0 import --force 30              # 强制重新导入
EOF
}

# 主函数
main() {
    case "${1:-help}" in
        start)
            start_collector
            ;;
        stop)
            stop_collector
            ;;
        status)
            status_collector
            ;;
        stats)
            show_stats
            ;;
        dashboard)
            show_dashboard "${2:-5}"
            ;;
        hourly)
            hourly_stats "${2:-24}"
            ;;
        sessions)
            session_stats
            ;;
        live)
            live_monitor "${2:-5}"
            ;;
        range)
            query_range "$2" "$3"
            ;;
        import)
            "${SCRIPT_DIR}/import-history.sh" "$2" "$3"
            ;;
        raw)
            show_raw
            ;;
        clean)
            clean_data
            ;;
        config)
            show_config_guide
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "未知命令: $1"
            show_help
            exit 1
            ;;
    esac
}

main "$@"
