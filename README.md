# cc-stats

Claude Code 用量统计工具 - 使用 OpenTelemetry Collector 收集数据，DuckDB 分析统计。

## 功能特性

- **实时数据采集** - 通过 OTEL Collector 接收 Claude Code 遥测数据
- **历史数据导入** - 从本地 `~/.claude/projects/` 导入历史会话记录
- **多维度统计** - Token 用量、费用、会话、模型、时间等多维分析
- **实时仪表盘** - TUI 界面实时监控用量
- **灵活查询** - 支持按时间范围、小时、会话等多种查询方式

## 快速开始

### 前置依赖

- Docker & Docker Compose
- [DuckDB](https://duckdb.org/) (需安装 `otlp` 社区扩展)
- jq

### 安装

```bash
git clone https://github.com/jimyag/cc-stats.git
cd cc-stats
chmod +x cc-stats.sh import-history.sh
```

### 配置 Claude Code

在 `~/.claude/settings.json` 中添加 OTEL 配置:

```json
{
  "telemetry": {
    "otel_endpoint": "http://localhost:4318"
  }
}
```

或设置环境变量:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"
```

### 基本使用

```bash
# 启动 OTEL Collector
./cc-stats.sh start

# 导入历史数据 (可选)
./cc-stats.sh import

# 查看统计
./cc-stats.sh stats

# 实时仪表盘
./cc-stats.sh dashboard
```

## 命令参考

| 命令 | 描述 |
|------|------|
| `start` | 启动 OTEL Collector |
| `stop` | 停止 OTEL Collector |
| `status` | 查看 Collector 状态 |
| `stats` | 查看完整用量统计 |
| `dashboard [N]` | 实时仪表盘 (默认每 5 秒刷新) |
| `hourly [N]` | 按小时统计 (默认最近 24 小时) |
| `sessions` | 查看会话详情 |
| `live [N]` | 实时监控 (默认最近 5 分钟) |
| `range <start> [end]` | 查询指定时间范围 |
| `import [N]` | 导入最近 N 天历史数据 (默认 30 天) |
| `import --force [N]` | 强制重新导入历史数据 |
| `raw` | 查看原始数据 |
| `clean` | 清理数据 |
| `config` | 显示 Claude Code 配置指南 |

## 使用示例

```bash
# 查看完整统计
./cc-stats.sh stats

# 每 2 秒刷新仪表盘
./cc-stats.sh dashboard 2

# 最近 48 小时按小时统计
./cc-stats.sh hourly 48

# 查询指定日期范围
./cc-stats.sh range '2026-01-01' '2026-01-31'

# 导入最近一年历史
./cc-stats.sh import 365

# 强制重新导入
./cc-stats.sh import --force 30
```

## 项目结构

```
cc-stats/
├── cc-stats.sh              # 主命令行工具
├── import-history.sh        # 历史数据导入脚本
├── docker-compose.yaml      # OTEL Collector 容器配置
├── otel-collector-config.yaml  # OTEL Collector 配置
├── stats.sql                # DuckDB 统计查询
├── stats-range.sql          # 时间范围查询
└── data/                    # 数据存储目录
    └── claude-metrics.jsonl # 指标数据
```

## 统计维度

### Token 用量

- Input Tokens - 输入 token 数
- Output Tokens - 输出 token 数
- Cache Read Tokens - 缓存读取 token 数
- Cache Creation Tokens - 缓存创建 token 数

### 费用统计

按模型分组的 USD 费用统计

### 时间维度

- 按天统计
- 按小时统计
- 自定义时间范围

### 会话统计

- 会话数量
- 会话时长
- 会话详情

## 技术架构

```
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────┐
│   Claude Code   │────>│   OTEL Collector     │────>│   JSONL     │
│   (遥测数据)     │     │   (Docker 容器)       │     │   文件       │
└─────────────────┘     └──────────────────────┘     └──────┬──────┘
                                                            │
┌─────────────────┐                                         v
│  历史会话文件    │─────────────────────────────────>┌─────────────┐
│ ~/.claude/      │  import-history.sh              │   DuckDB    │
└─────────────────┘                                  │   (分析)     │
                                                     └─────────────┘
```

## License

MIT
