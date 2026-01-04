-- Claude Code 时间范围查询
-- 使用方式: duckdb -c ".read stats-range.sql" -c "SET start_time='2026-01-01'; SET end_time='2026-01-31';"

INSTALL otlp FROM community;
LOAD otlp;

.mode markdown

SET VARIABLE metrics_path = '/Users/jimyag/src/github/jimyag/cc-stats/data/claude-metrics.jsonl';

-- 使用传入的参数或默认值
SET VARIABLE start_ts = COALESCE(TRY_CAST(getvariable('start_time') AS TIMESTAMP), NOW() - INTERVAL '7 days');
SET VARIABLE end_ts = COALESCE(TRY_CAST(getvariable('end_time') AS TIMESTAMP), NOW());

SELECT '=== 查询时间范围 ===' as info;
SELECT getvariable('start_ts') as start_time, getvariable('end_ts') as end_time;

SELECT '=== Token 用量 ===' as info;

SELECT
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens,
    SUM(CASE WHEN Attributes['type'] = 'cacheRead' THEN Value ELSE 0 END) as cache_read_tokens
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage'
  AND Timestamp::TIMESTAMP BETWEEN getvariable('start_ts') AND getvariable('end_ts');

SELECT '=== 费用统计 ===' as info;

SELECT
    COALESCE(Attributes['model'], 'unknown') as model,
    ROUND(SUM(Value), 4) as cost_usd
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.cost.usage'
  AND Timestamp::TIMESTAMP BETWEEN getvariable('start_ts') AND getvariable('end_ts')
GROUP BY 1;

SELECT '=== 按天明细 ===' as info;

SELECT
    DATE_TRUNC('day', Timestamp::TIMESTAMP)::DATE as date,
    SUM(CASE WHEN Attributes['type'] = 'input' THEN Value ELSE 0 END) as input_tokens,
    SUM(CASE WHEN Attributes['type'] = 'output' THEN Value ELSE 0 END) as output_tokens
FROM read_otlp_metrics(getvariable('metrics_path'))
WHERE MetricName = 'claude_code.token.usage'
  AND Timestamp::TIMESTAMP BETWEEN getvariable('start_ts') AND getvariable('end_ts')
GROUP BY 1
ORDER BY 1;
