-- Claude Code 时间范围查询
-- 需要通过 -c "SET VARIABLE metrics_path = '...'" 传入路径

INSTALL otlp FROM community;
LOAD otlp;

.mode markdown

-- metrics_path 需要从外部传入
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
