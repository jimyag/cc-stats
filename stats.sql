-- Claude Code Token 用量统计
-- 使用 DuckDB OTLP 扩展解析 OTEL 导出的数据
-- 需要通过 -c "SET VARIABLE metrics_path = '...'" 传入路径

INSTALL otlp FROM community;
LOAD otlp;

.mode markdown

-- 设置时区为本地时区
SET TimeZone = 'Asia/Shanghai';

-- metrics_path 需要从外部传入

-- ============================================
-- Token 用量统计
-- ============================================

SELECT '=== 总用量统计 ===' as info;

SELECT
    COUNT(*) as total_records,
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens,
    SUM(CASE WHEN Attributes['type'] = 'cacheRead' THEN Value ELSE 0 END) as cache_read_tokens,
    SUM(CASE WHEN Attributes['type'] = 'cacheCreation' THEN Value ELSE 0 END) as cache_creation_tokens
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage';

-- ============================================
-- 按日期统计
-- ============================================

SELECT '=== 近 7 天用量 ===' as info;

SELECT
    DATE_TRUNC('day', Timestamp::TIMESTAMP)::DATE as date,
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens,
    SUM(CASE WHEN Attributes['type'] = 'cacheRead' THEN Value ELSE 0 END) as cache_read_tokens
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage'
  AND Timestamp::TIMESTAMP > NOW() - INTERVAL '7 days'
GROUP BY 1
ORDER BY 1 DESC;

-- ============================================
-- 按模型统计
-- ============================================

SELECT '=== 按模型统计 ===' as info;

SELECT
    COALESCE(Attributes['model'], 'unknown') as model,
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens,
    SUM(CASE WHEN Attributes['type'] = 'cacheRead' THEN Value ELSE 0 END) as cache_read_tokens
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage'
GROUP BY 1
ORDER BY 2 DESC;


-- ============================================
-- 会话统计
-- ============================================

SELECT '=== 会话统计 ===' as info;

SELECT
    DATE_TRUNC('day', Timestamp::TIMESTAMP)::DATE as date,
    COUNT(*) as session_count
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.session.count'
GROUP BY 1
ORDER BY 1 DESC;

-- ============================================
-- 按小时统计
-- ============================================

SELECT '=== 按小时统计 (近24小时) ===' as info;

SELECT
    DATE_TRUNC('hour', (Timestamp::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai') as hour,
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage'
  AND Timestamp::TIMESTAMP > NOW() - INTERVAL '24 hours'
GROUP BY 1
ORDER BY 1 DESC;

-- ============================================
-- 按会话时间线
-- ============================================

SELECT '=== 会话时间线 ===' as info;

SELECT
    Attributes['session.id'] as session_id,
    (MIN(Timestamp::TIMESTAMP) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai' as start_time,
    (MAX(Timestamp::TIMESTAMP) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai' as last_activity,
    COALESCE(Attributes['model'], 'unknown') as model,
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage'
GROUP BY 1, 4
ORDER BY 2 DESC
LIMIT 20;

-- ============================================
-- 活跃时间统计
-- ============================================

SELECT '=== 活跃时间统计 ===' as info;

SELECT
    COALESCE(Attributes['type'], 'unknown') as time_type,
    ROUND(SUM(Value) / 3600, 2) as total_hours,
    ROUND(SUM(Value) / 60, 1) as total_minutes
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.active_time.total'
GROUP BY 1
ORDER BY 2 DESC;

-- ============================================
-- 代码行数变更统计
-- ============================================

SELECT '=== 代码行数变更 ===' as info;

SELECT
    DATE_TRUNC('day', Timestamp::TIMESTAMP)::DATE as date,
    SUM(CASE WHEN Attributes['type'] = 'added' THEN Value ELSE 0 END) as lines_added,
    SUM(CASE WHEN Attributes['type'] = 'removed' THEN Value ELSE 0 END) as lines_removed
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.lines_of_code.count'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 14;

-- ============================================
-- Git 活动统计
-- ============================================

SELECT '=== Git 活动统计 ===' as info;

SELECT
    DATE_TRUNC('day', Timestamp::TIMESTAMP)::DATE as date,
    SUM(CASE WHEN MetricName = 'claude_code.commit.count' THEN Value ELSE 0 END) as commits,
    SUM(CASE WHEN MetricName = 'claude_code.pull_request.count' THEN Value ELSE 0 END) as pull_requests
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName IN ('claude_code.commit.count', 'claude_code.pull_request.count')
GROUP BY 1
ORDER BY 1 DESC
LIMIT 14;

-- ============================================
-- 编辑工具使用统计
-- ============================================

SELECT '=== 编辑工具统计 ===' as info;

SELECT
    COALESCE(Attributes['tool'], 'unknown') as tool,
    COALESCE(Attributes['decision'], 'unknown') as decision,
    COALESCE(Attributes['language'], 'unknown') as language,
    SUM(Value) as count
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.code_edit_tool.decision'
GROUP BY 1, 2, 3
ORDER BY 4 DESC
LIMIT 20;

-- ============================================
-- 按终端类型统计
-- ============================================

SELECT '=== 按终端类型统计 ===' as info;

SELECT
    COALESCE(Attributes['terminal.type'], 'unknown') as terminal,
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage'
GROUP BY 1
ORDER BY 2 DESC;

-- ============================================
-- 实时监控 (最近 10 分钟)
-- ============================================

SELECT '=== 实时监控 (最近10分钟) ===' as info;

SELECT
    DATE_TRUNC('minute', (Timestamp::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai') as minute,
    MetricName,
    ROUND(SUM(Value), 2) as value
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE Timestamp::TIMESTAMP > NOW() - INTERVAL '10 minutes'
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- ============================================
-- 所有指标类型
-- ============================================

SELECT '=== 可用指标 ===' as info;

SELECT DISTINCT MetricName, COUNT(*) as count
FROM read_otlp_metrics(getvariable('metrics_path'))
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;
