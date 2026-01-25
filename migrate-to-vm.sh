#!/bin/bash

# 将 OTEL JSONL 历史数据迁移到 VictoriaMetrics 和 VictoriaLogs
# 使用 OTLP JSON 格式直接发送到 VM/VMLogs 的 OpenTelemetry 端点

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
METRICS_FILE="${DATA_DIR}/claude-metrics.jsonl"
LOGS_FILE="${DATA_DIR}/claude-logs.jsonl"

# 默认配置
# VictoriaMetrics: OTLP 端点只支持 protobuf，改用 /api/v1/import 原生 JSON 格式
VM_IMPORT_URL="${VM_IMPORT_URL:-http://vm.example.com/api/v1/import}"
# VictoriaLogs: OTLP 端点只支持 protobuf，改用 jsonline 端点
VMLOGS_JSONLINE_URL="${VMLOGS_JSONLINE_URL:-http://vmlogs.example.com/insert/jsonline}"
BATCH_SIZE="${BATCH_SIZE:-100}"

# 自定义属性
EXTRA_RESOURCE_ATTRS='{"key":"job","value":{"stringValue":"claude"}}'

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# 检查依赖
check_deps() {
    local missing=()
    for cmd in jq curl; do
        if ! command -v "$cmd" &> /dev/null; then
            missing+=("$cmd")
        fi
    done

    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "缺少依赖: ${missing[*]}"
        exit 1
    fi
}

# 检查服务连通性
check_connectivity() {
    local url="$1"
    local name="$2"

    # 从 OTLP URL 提取基础 URL 进行健康检查
    local base_url
    base_url=$(echo "$url" | sed 's|/opentelemetry/.*||; s|/insert/.*||')

    if curl -s --connect-timeout 5 "${base_url}/health" > /dev/null 2>&1 || \
       curl -s --connect-timeout 5 "${base_url}/-/healthy" > /dev/null 2>&1 || \
       curl -s --connect-timeout 5 -o /dev/null -w "%{http_code}" "$url" 2>&1 | grep -qE "^(200|204|405)$"; then
        log_info "$name 连接成功"
        log_info "  端点: $url"
        return 0
    else
        log_error "$name 无法连接: $url"
        return 1
    fi
}

# 为 OTEL metrics 添加自定义 resource 属性 (job=claude)
add_resource_attrs_metrics() {
    local input_file="$1"

    jq -c --argjson extra_attr "$EXTRA_RESOURCE_ATTRS" '
        .resourceMetrics[]?.resource.attributes += [$extra_attr] |
        .
    ' "$input_file" 2>/dev/null
}

# 为 OTEL logs 添加自定义 resource 属性 (job=claude)
add_resource_attrs_logs() {
    local input_file="$1"

    jq -c --argjson extra_attr "$EXTRA_RESOURCE_ATTRS" '
        .resourceLogs[]?.resource.attributes += [$extra_attr] |
        .
    ' "$input_file" 2>/dev/null
}

# 将 OTEL logs 转换为 VictoriaLogs jsonline 格式
# 保持与 OTLP protobuf 导入的数据格式一致
convert_logs_to_jsonline() {
    local input_file="$1"

    jq -c '
        .resourceLogs[]? |
        .resource.attributes as $resource_attrs |
        (
            [$resource_attrs[]? | {(.key): .value.stringValue}] | add // {}
        ) as $resource |
        .scopeLogs[]? |
        .scope as $scope |
        .logRecords[]? |

        # 提取 attributes
        (
            [.attributes[]? | {(.key): .value.stringValue}] | add // {}
        ) as $attrs |

        # 时间戳从纳秒转换为 RFC3339 (毫秒精度)
        ((.timeUnixNano | tonumber) / 1000000000) as $ts_sec |

        # 构建日志记录 - 保持与 OTLP 导入格式一致
        {
            "_msg": .body.stringValue,
            "_time": ($ts_sec | todate),

            # resource attributes
            "host.arch": $resource["host.arch"],
            "job": "claude",
            "os.type": $resource["os.type"],
            "os.version": $resource["os.version"],
            "service.name": $resource["service.name"],
            "service.version": $resource["service.version"],

            # scope info
            "scope.name": ($scope.name // "com.anthropic.claude_code.events"),
            "scope.version": ($scope.version // $resource["service.version"]),

            # log attributes - 保持点号格式
            "session.id": $attrs["session.id"],
            "user.id": $attrs["user.id"],
            "terminal.type": $attrs["terminal.type"],
            "event.name": $attrs["event.name"],
            "event.timestamp": $attrs["event.timestamp"],

            # 其他属性
            "model": $attrs.model,
            "input_tokens": $attrs.input_tokens,
            "output_tokens": $attrs.output_tokens,
            "cache_read_tokens": $attrs.cache_read_tokens,
            "cache_creation_tokens": $attrs.cache_creation_tokens,
            "cost_usd": $attrs.cost_usd,
            "duration_ms": $attrs.duration_ms,
            "tool_name": $attrs.tool_name,
            "success": $attrs.success,
            "decision": $attrs.decision,
            "source": $attrs.source,
            "decision_source": $attrs.decision_source,
            "decision_type": $attrs.decision_type,
            "tool_result_size_bytes": $attrs.tool_result_size_bytes,
            "prompt_length": $attrs.prompt_length,
            "error": $attrs.error
        } |
        # 移除 null 值
        with_entries(select(.value != null))
    ' "$input_file" 2>/dev/null
}

# 将 OTEL metrics 转换为 VictoriaMetrics 原生 JSON 格式
# VM 格式: {"metric":{"__name__":"name","label":"value"},"values":[v1],"timestamps":[t1]}
convert_metrics_to_vm_json() {
    local input_file="$1"

    jq -c '
        .resourceMetrics[]? |
        .resource.attributes as $resource_attrs |
        (
            [$resource_attrs[]? | {(.key): .value.stringValue}] | add // {}
        ) as $resource |
        .scopeMetrics[]? |
        .scope as $scope |
        .metrics[]? |
        . as $metric |

        # 处理 sum 类型的 metric
        if .sum then
            .sum.dataPoints[]? |

            # 提取 datapoint attributes
            (
                [.attributes[]? | {(.key): .value.stringValue}] | add // {}
            ) as $dp_attrs |

            # 提取时间戳 (从纳秒转换为毫秒)
            ((.timeUnixNano | tonumber) / 1000000 | floor) as $ts_ms |

            # 提取值
            (.asDouble // .asInt // 0) as $value |

            # 构建 VM 原生 JSON 格式
            {
                metric: (
                    {
                        "__name__": $metric.name,
                        "job": "claude",
                        # resource attributes
                        "host.arch": $resource["host.arch"],
                        "os.type": $resource["os.type"],
                        "os.version": $resource["os.version"],
                        "service.name": $resource["service.name"],
                        "service.version": $resource["service.version"],
                        # scope info
                        "scope.name": ($scope.name // "com.anthropic.claude_code"),
                        "scope.version": ($scope.version // $resource["service.version"]),
                        # datapoint attributes
                        "user.id": $dp_attrs["user.id"],
                        "session.id": $dp_attrs["session.id"],
                        "terminal.type": $dp_attrs["terminal.type"],
                        "type": $dp_attrs["type"],
                        "model": $dp_attrs["model"]
                    } | with_entries(select(.value != null))
                ),
                values: [$value],
                timestamps: [$ts_ms]
            }
        elif .gauge then
            .gauge.dataPoints[]? |

            (
                [.attributes[]? | {(.key): .value.stringValue}] | add // {}
            ) as $dp_attrs |

            ((.timeUnixNano | tonumber) / 1000000 | floor) as $ts_ms |
            (.asDouble // .asInt // 0) as $value |

            {
                metric: (
                    {
                        "__name__": $metric.name,
                        "job": "claude",
                        "host.arch": $resource["host.arch"],
                        "os.type": $resource["os.type"],
                        "os.version": $resource["os.version"],
                        "service.name": $resource["service.name"],
                        "service.version": $resource["service.version"],
                        "scope.name": ($scope.name // "com.anthropic.claude_code"),
                        "scope.version": ($scope.version // $resource["service.version"]),
                        "user.id": $dp_attrs["user.id"],
                        "session.id": $dp_attrs["session.id"],
                        "terminal.type": $dp_attrs["terminal.type"],
                        "type": $dp_attrs["type"],
                        "model": $dp_attrs["model"]
                    } | with_entries(select(.value != null))
                ),
                values: [$value],
                timestamps: [$ts_ms]
            }
        else
            empty
        end
    ' "$input_file" 2>/dev/null
}

# 导入 metrics 到 VictoriaMetrics (使用原生 JSON 格式)
import_metrics() {
    local input_file="$1"

    if [[ ! -f "$input_file" ]]; then
        log_warn "Metrics 文件不存在: $input_file"
        return 1
    fi

    local total_lines=$(wc -l < "$input_file" | tr -d ' ')
    log_step "开始导入 Metrics 到 VictoriaMetrics..."
    log_info "源文件: $input_file ($total_lines 行 OTEL 记录)"
    log_info "目标端点: $VM_IMPORT_URL"

    local temp_file=$(mktemp)
    local batch_num=0
    local success_count=0
    local failed=0

    # 转换为 VM 原生 JSON 格式
    log_info "转换 OTEL 格式到 VictoriaMetrics 原生 JSON 格式..."
    convert_metrics_to_vm_json "$input_file" > "$temp_file"

    local converted=$(wc -l < "$temp_file" | tr -d ' ')
    log_info "转换完成: $converted 条 metric 数据点"

    if [[ $converted -eq 0 ]]; then
        log_warn "没有转换出有效的 metrics 数据"
        rm -f "$temp_file"
        return 1
    fi

    # 批量导入
    log_info "开始批量导入..."

    # 创建临时目录存放批次文件
    local batch_dir=$(mktemp -d)
    split -l "$BATCH_SIZE" "$temp_file" "${batch_dir}/batch."

    for batch_file in "${batch_dir}"/batch.*; do
        [[ -f "$batch_file" ]] || continue
        ((batch_num++)) || true
        local batch_lines=$(wc -l < "$batch_file" | tr -d ' ')

        local response
        response=$(curl -s -w "\n%{http_code}" -X POST \
            --retry 3 --retry-delay 2 --retry-all-errors \
            -H 'Content-Type: application/json' \
            --data-binary "@$batch_file" \
            "$VM_IMPORT_URL" 2>&1) || true

        local http_code=$(echo "$response" | tail -1)
        local body=$(echo "$response" | sed '$d')

        if [[ "$http_code" == "204" ]] || [[ "$http_code" == "200" ]]; then
            ((success_count += batch_lines)) || true
            printf "\r${GREEN}[INFO]${NC} 批次 %d: 导入 %d 条成功 (总计: %d/%d)" \
                "$batch_num" "$batch_lines" "$success_count" "$converted"
        else
            ((failed += batch_lines)) || true
            log_error "批次 $batch_num 导入失败 (HTTP $http_code): $body"
        fi
    done

    echo ""
    rm -rf "$batch_dir" "$temp_file"

    log_info "Metrics 导入完成: 成功 $success_count, 失败 $failed"
    return 0
}

# 导入 logs 到 VictoriaLogs (使用 jsonline 格式)
import_logs() {
    local input_file="$1"

    if [[ ! -f "$input_file" ]]; then
        log_warn "Logs 文件不存在: $input_file"
        return 1
    fi

    local total_lines=$(wc -l < "$input_file" | tr -d ' ')
    log_step "开始导入 Logs 到 VictoriaLogs..."
    log_info "源文件: $input_file ($total_lines 行 OTEL 记录)"
    log_info "目标端点: $VMLOGS_JSONLINE_URL"

    local temp_file=$(mktemp)
    local batch_num=0
    local success_count=0
    local failed=0

    # 转换为 VictoriaLogs jsonline 格式
    log_info "转换 OTEL 格式到 VictoriaLogs jsonline 格式..."
    convert_logs_to_jsonline "$input_file" > "$temp_file"

    local converted=$(wc -l < "$temp_file" | tr -d ' ')
    log_info "转换完成: $converted 条日志记录"

    if [[ $converted -eq 0 ]]; then
        log_warn "没有转换出有效的日志数据"
        rm -f "$temp_file"
        return 1
    fi

    # 批量导入 - 使用 jsonline 格式
    log_info "开始批量导入..."

    # 创建临时目录存放批次文件
    local batch_dir=$(mktemp -d)
    split -l "$BATCH_SIZE" "$temp_file" "${batch_dir}/batch."

    for batch_file in "${batch_dir}"/batch.*; do
        [[ -f "$batch_file" ]] || continue
        ((batch_num++)) || true
        local batch_lines=$(wc -l < "$batch_file" | tr -d ' ')

        local response
        response=$(curl -s -w "\n%{http_code}" -X POST \
            --retry 3 --retry-delay 2 --retry-all-errors \
            -H 'Content-Type: application/stream+json' \
            --data-binary "@$batch_file" \
            "${VMLOGS_JSONLINE_URL}?_stream_fields=job,service,session_id&_msg_field=_msg&_time_field=_time" 2>&1) || true

        local http_code=$(echo "$response" | tail -1)
        local body=$(echo "$response" | sed '$d')

        if [[ "$http_code" == "204" ]] || [[ "$http_code" == "200" ]]; then
            ((success_count += batch_lines)) || true
            printf "\r${GREEN}[INFO]${NC} 批次 %d: 导入 %d 条成功 (总计: %d/%d)" \
                "$batch_num" "$batch_lines" "$success_count" "$converted"
        else
            ((failed += batch_lines)) || true
            log_error "批次 $batch_num 导入失败 (HTTP $http_code): $body"
        fi
    done

    echo ""
    rm -rf "$batch_dir" "$temp_file"

    log_info "Logs 导入完成: 成功 $success_count, 失败 $failed"
    return 0
}

# 预览转换结果
preview() {
    local type="$1"
    local count="${2:-5}"

    case "$type" in
        metrics)
            if [[ -f "$METRICS_FILE" ]]; then
                log_info "预览 Metrics 转换结果 (前 $count 条，VictoriaMetrics JSON 格式):"
                convert_metrics_to_vm_json "$METRICS_FILE" | head -n "$count" | jq .
            else
                log_error "Metrics 文件不存在: $METRICS_FILE"
            fi
            ;;
        logs)
            if [[ -f "$LOGS_FILE" ]]; then
                log_info "预览 Logs 转换结果 (前 $count 条，VictoriaLogs jsonline 格式):"
                convert_logs_to_jsonline "$LOGS_FILE" | head -n "$count" | jq .
            else
                log_error "Logs 文件不存在: $LOGS_FILE"
            fi
            ;;
        *)
            log_error "未知类型: $type (可选: metrics, logs)"
            ;;
    esac
}

# 显示帮助
show_help() {
    cat << EOF
将 OTEL JSONL 历史数据迁移到 VictoriaMetrics 和 VictoriaLogs
使用 OTLP JSON 格式直接发送到 OpenTelemetry 端点

用法: $0 <command> [options]

命令:
    all                 导入所有数据 (metrics + logs)
    metrics             仅导入 metrics 到 VictoriaMetrics
    logs                仅导入 logs 到 VictoriaLogs
    preview <type>      预览数据 (type: metrics 或 logs)
    check               检查服务连通性
    help                显示帮助

环境变量:
    VM_IMPORT_URL       VictoriaMetrics 导入端点
                        (默认: http://vm.example.com/api/v1/import)
    VMLOGS_JSONLINE_URL VictoriaLogs jsonline 端点
                        (默认: http://vmlogs.example.com/insert/jsonline)
    BATCH_SIZE          批量导入大小 (默认: 100)

自定义属性:
    所有导入的数据会自动添加 job=claude 属性

示例:
    $0 check                              # 检查服务连通性
    $0 preview metrics                    # 预览 metrics 数据
    $0 preview logs                       # 预览 logs 数据
    $0 metrics                            # 导入 metrics
    $0 logs                               # 导入 logs
    $0 all                                # 导入所有数据

    # 使用自定义端点
    VM_IMPORT_URL=http://localhost:8428/api/v1/import $0 metrics

数据源:
    Metrics: ${METRICS_FILE}
    Logs:    ${LOGS_FILE}
EOF
}

# 主函数
main() {
    check_deps

    case "${1:-help}" in
        all)
            if ! check_connectivity "$VM_IMPORT_URL" "VictoriaMetrics"; then
                exit 1
            fi
            if ! check_connectivity "$VMLOGS_JSONLINE_URL" "VictoriaLogs"; then
                exit 1
            fi
            echo ""
            import_metrics "$METRICS_FILE"
            echo ""
            import_logs "$LOGS_FILE"
            echo ""
            log_info "全部导入完成!"
            ;;
        metrics)
            if ! check_connectivity "$VM_IMPORT_URL" "VictoriaMetrics"; then
                exit 1
            fi
            import_metrics "$METRICS_FILE"
            ;;
        logs)
            if ! check_connectivity "$VMLOGS_JSONLINE_URL" "VictoriaLogs"; then
                exit 1
            fi
            import_logs "$LOGS_FILE"
            ;;
        preview)
            preview "${2:-metrics}" "${3:-5}"
            ;;
        check)
            log_step "检查服务连通性..."
            check_connectivity "$VM_IMPORT_URL" "VictoriaMetrics" || true
            check_connectivity "$VMLOGS_JSONLINE_URL" "VictoriaLogs" || true
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "未知命令: $1"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

main "$@"
