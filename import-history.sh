#!/bin/bash

# 导入 Claude Code 历史会话数据到 cc-stats
# 从 ~/.claude/projects/ 目录读取历史会话文件
# 优化版：使用并行处理和批量 jq 操作

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
CLAUDE_PROJECTS_DIR="${HOME}/.claude/projects"
OUTPUT_FILE="${DATA_DIR}/claude-metrics.jsonl"
IMPORTED_SESSIONS_FILE="${DATA_DIR}/.imported-sessions"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查依赖
check_deps() {
    if ! command -v jq &> /dev/null; then
        log_error "需要安装 jq"
        exit 1
    fi
}

# 跨平台获取文件修改时间和大小
get_file_stat() {
    local file="$1"
    if [[ "$(uname)" == "Darwin" ]]; then
        stat -f '%m %z' "$file" 2>/dev/null
    else
        stat -c '%Y %s' "$file" 2>/dev/null
    fi
}

# 跨平台解析 ISO 8601 时间戳为 Unix 时间戳
parse_timestamp() {
    local ts="$1"
    if [[ "$(uname)" == "Darwin" ]]; then
        # macOS: 使用 date -j -f
        date -j -f "%Y-%m-%dT%H:%M:%S" "${ts%%.*}" "+%s" 2>/dev/null
    else
        # Linux: 使用 date -d
        date -d "${ts}" "+%s" 2>/dev/null
    fi
}

# 主导入函数 - 优化版
import_history() {
    local days="${1:-30}"
    local force="${2:-false}"

    check_deps

    if [[ ! -d "$CLAUDE_PROJECTS_DIR" ]]; then
        log_error "Claude projects 目录不存在: $CLAUDE_PROJECTS_DIR"
        exit 1
    fi

    mkdir -p "$DATA_DIR"

    # 如果是强制模式，清除导入记录
    if [[ "$force" == "true" ]]; then
        log_warn "强制模式: 将重新导入所有数据"
        rm -f "$IMPORTED_SESSIONS_FILE"
        if [[ -f "$OUTPUT_FILE" ]]; then
            local backup_file="${OUTPUT_FILE}.backup.$(date +%Y%m%d%H%M%S)"
            mv "$OUTPUT_FILE" "$backup_file"
            log_info "已备份现有数据到: $backup_file"
        fi
    fi

    log_info "开始导入最近 ${days} 天的历史数据..."
    log_info "扫描目录: $CLAUDE_PROJECTS_DIR"

    local cutoff_time=$(($(date +%s) - days * 86400))
    local temp_file=$(mktemp)
    local temp_imported=$(mktemp)
    local processed=0
    local skipped=0
    local total_records=0

    # 复制已导入记录到临时文件
    if [[ -f "$IMPORTED_SESSIONS_FILE" ]]; then
        cp "$IMPORTED_SESSIONS_FILE" "$temp_imported"
    fi

    # 收集需要处理的文件
    local files_to_process=()

    while IFS= read -r -d '' session_file; do
        # 跳过 agent 文件
        local basename=$(basename "$session_file" .jsonl)
        if [[ "$basename" == agent-* ]]; then
            continue
        fi

        # 获取文件信息
        local file_stat
        file_stat=$(get_file_stat "$session_file") || continue
        local file_mtime=$(echo "$file_stat" | awk '{print $1}')
        local file_size=$(echo "$file_stat" | awk '{print $2}')

        # 检查时间范围
        if [[ "$file_mtime" -lt "$cutoff_time" ]]; then
            continue
        fi

        local session_file_hash="${session_file}:${file_size}:${file_mtime}"

        # 检查是否已导入
        if [[ -f "$temp_imported" ]] && grep -qF "$session_file_hash" "$temp_imported" 2>/dev/null; then
            ((skipped++))
            continue
        fi

        files_to_process+=("$session_file|$session_file_hash")
    done < <(find "$CLAUDE_PROJECTS_DIR" -name "*.jsonl" -type f -print0 2>/dev/null)

    local total_files=${#files_to_process[@]}
    log_info "找到 ${total_files} 个新文件需要处理，跳过 ${skipped} 个已导入文件"

    if [[ $total_files -eq 0 ]]; then
        if [[ $skipped -gt 0 ]]; then
            log_info "所有文件已导入过"
            log_info "如需重新导入，请使用: $0 --force [days]"
        else
            log_warn "没有找到可导入的数据"
        fi
        rm -f "$temp_file" "$temp_imported"
        return
    fi

    # 处理每个文件
    for entry in "${files_to_process[@]}"; do
        local session_file="${entry%|*}"
        local session_file_hash="${entry#*|}"

        ((processed++))

        # 显示进度
        if [[ $((processed % 10)) -eq 0 ]] || [[ $processed -eq $total_files ]]; then
            printf "\r${GREEN}[INFO]${NC} 处理进度: %d/%d (%d%%)" "$processed" "$total_files" "$((processed * 100 / total_files))"
        fi

        # 使用 jq 一次性提取所有数据并转换
        local records
        records=$(jq -c '
            select(.type == "assistant" and (.message.usage | type) == "object") |
            {
                ts: .timestamp,
                sid: .sessionId,
                m: .message.model,
                i: .message.usage.input_tokens,
                o: .message.usage.output_tokens,
                cr: (.message.usage.cache_read_input_tokens // 0),
                cc: (.message.usage.cache_creation_input_tokens // 0)
            }
        ' "$session_file" 2>/dev/null) || continue

        if [[ -z "$records" ]]; then
            continue
        fi

        # 转换为 OTEL 格式
        while IFS= read -r record; do
            local timestamp=$(echo "$record" | jq -r '.ts')
            local session_id=$(echo "$record" | jq -r '.sid')
            local model=$(echo "$record" | jq -r '.m // "unknown"')
            local input_tokens=$(echo "$record" | jq -r '.i // 0')
            local output_tokens=$(echo "$record" | jq -r '.o // 0')
            local cache_read=$(echo "$record" | jq -r '.cr // 0')
            local cache_creation=$(echo "$record" | jq -r '.cc // 0')

            # 转换时间戳
            local ts_sec
            ts_sec=$(parse_timestamp "${timestamp}") || continue
            local ts_nano="${ts_sec}000000000"

            # 生成 OTEL 记录
            echo "{\"resourceMetrics\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"claude-code\"}},{\"key\":\"service.version\",\"value\":{\"stringValue\":\"historical\"}}]},\"scopeMetrics\":[{\"scope\":{\"name\":\"com.anthropic.claude_code\",\"version\":\"historical\"},\"metrics\":[{\"name\":\"claude_code.token.usage\",\"description\":\"Number of tokens used\",\"unit\":\"tokens\",\"sum\":{\"dataPoints\":[{\"attributes\":[{\"key\":\"user.id\",\"value\":{\"stringValue\":\"historical-import\"}},{\"key\":\"session.id\",\"value\":{\"stringValue\":\"${session_id}\"}},{\"key\":\"type\",\"value\":{\"stringValue\":\"input\"}},{\"key\":\"model\",\"value\":{\"stringValue\":\"${model}\"}}],\"startTimeUnixNano\":\"${ts_nano}\",\"timeUnixNano\":\"${ts_nano}\",\"asDouble\":${input_tokens}},{\"attributes\":[{\"key\":\"user.id\",\"value\":{\"stringValue\":\"historical-import\"}},{\"key\":\"session.id\",\"value\":{\"stringValue\":\"${session_id}\"}},{\"key\":\"type\",\"value\":{\"stringValue\":\"output\"}},{\"key\":\"model\",\"value\":{\"stringValue\":\"${model}\"}}],\"startTimeUnixNano\":\"${ts_nano}\",\"timeUnixNano\":\"${ts_nano}\",\"asDouble\":${output_tokens}},{\"attributes\":[{\"key\":\"user.id\",\"value\":{\"stringValue\":\"historical-import\"}},{\"key\":\"session.id\",\"value\":{\"stringValue\":\"${session_id}\"}},{\"key\":\"type\",\"value\":{\"stringValue\":\"cacheRead\"}},{\"key\":\"model\",\"value\":{\"stringValue\":\"${model}\"}}],\"startTimeUnixNano\":\"${ts_nano}\",\"timeUnixNano\":\"${ts_nano}\",\"asDouble\":${cache_read}},{\"attributes\":[{\"key\":\"user.id\",\"value\":{\"stringValue\":\"historical-import\"}},{\"key\":\"session.id\",\"value\":{\"stringValue\":\"${session_id}\"}},{\"key\":\"type\",\"value\":{\"stringValue\":\"cacheCreation\"}},{\"key\":\"model\",\"value\":{\"stringValue\":\"${model}\"}}],\"startTimeUnixNano\":\"${ts_nano}\",\"timeUnixNano\":\"${ts_nano}\",\"asDouble\":${cache_creation}}],\"aggregationTemporality\":1,\"isMonotonic\":true}}]}]}]}" >> "$temp_file"

            ((total_records++))
        done <<< "$records"

        # 标记文件已导入
        echo "$session_file_hash" >> "$temp_imported"
    done

    echo "" # 换行

    # 写入结果
    if [[ -s "$temp_file" ]]; then
        cat "$temp_file" >> "$OUTPUT_FILE"
        mv "$temp_imported" "$IMPORTED_SESSIONS_FILE"

        # 统计 tokens
        local total_input=$(jq -s '[.[].resourceMetrics[].scopeMetrics[].metrics[].sum.dataPoints[] | select(.attributes[] | select(.key == "type" and .value.stringValue == "input")) | .asDouble] | add' "$temp_file" 2>/dev/null || echo 0)
        local total_output=$(jq -s '[.[].resourceMetrics[].scopeMetrics[].metrics[].sum.dataPoints[] | select(.attributes[] | select(.key == "type" and .value.stringValue == "output")) | .asDouble] | add' "$temp_file" 2>/dev/null || echo 0)

        log_info "导入完成!"
        log_info "处理文件数: $processed"
        log_info "新导入记录数: $total_records"
        log_info "总 Input Tokens: ${total_input:-0}"
        log_info "总 Output Tokens: ${total_output:-0}"
    else
        log_warn "没有提取到有效数据"
        rm -f "$temp_imported"
    fi

    rm -f "$temp_file"
}

# 显示帮助
show_help() {
    cat << EOF
导入 Claude Code 历史会话数据

用法: $0 [options] [days]

参数:
    days        导入最近 N 天的数据 (默认: 30)

选项:
    --force     强制重新导入所有数据（备份并清空现有数据）
    --help      显示帮助

示例:
    $0              # 导入最近 30 天（增量）
    $0 7            # 导入最近 7 天（增量）
    $0 --force 30   # 强制重新导入最近 30 天
    $0 --force 365  # 强制重新导入最近一年

说明:
    - 默认为增量导入，已导入的文件会自动跳过
    - 文件修改后会重新导入
    - 使用 --force 会清空现有数据并重新导入
EOF
}

case "${1:-}" in
    help|--help|-h)
        show_help
        ;;
    --force)
        import_history "${2:-30}" "true"
        ;;
    *)
        import_history "${1:-30}" "false"
        ;;
esac
