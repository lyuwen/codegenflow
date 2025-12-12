# 推理流程 (Reasoning Pipeline)

用于验证、标注和管理推理响应的数据处理流程。

## 设置 (Setup)

1. **克隆仓库** 并 **安装依赖**:
   ```bash
   pip install -r requirements.txt
   ```

2. **环境变量**:
   在根目录下创建一个 `.env` 文件:
   ```env
   # PostgreSQL
   DB_URL=postgresql://user:password@host:port/database
   
   # 或 SQLite
   DB_URL=sqlite:///problems.db
   ```

## 使用方法: `run_pipeline.py`

主入口程序是 `run_pipeline.py`。它支持多种子命令:

### 1. 验证响应 (Verify Responses)
在沙箱环境中运行代码验证。

**在线验证 (Online Verification)**:
```bash
python run_pipeline.py verify --limit 1000
```

**离线验证 (Offline Verification)**:
将任务导出到文件以进行离线处理，然后导入结果。
```bash
# 1. 导出任务
python run_pipeline.py verify --dump-tasks tasks.jsonl --limit 10000

# 2. (外部流程: 在沙箱中运行任务并保存到 results.jsonl)

# 3. 导入结果
python run_pipeline.py verify --ingest-results results.jsonl
```

**选项**:
- `--concurrency`: 并行工作线程数 (默认: 8)。
- `--dryrun`: 验证但不更新数据库。
- `--retry-status`: 重试特定状态 (例如 `failed,error`)。
- `--dump-tasks`: 导出验证任务的路径。
- `--ingest-results`: 导入验证结果的路径 (支持多个)。

### 2. 导入数据 (Import Data)
从 JSONL 文件导入响应或问题。
```bash
python run_pipeline.py import --pattern "data/*.jsonl"
python run_pipeline.py import-problems --pattern "problems/*.jsonl"
```

### 3. 标注 (Annotate)
计算指标，如重复率 (CR, LRR)、熵和推理缺陷。
```bash
python run_pipeline.py annotate --limit 5000 --concurrency 16
```
**选项**:
- `--redo`: 强制重新标注已标注的响应。

### 4. 更新状态 (Update Status)
批量更新响应的验证状态。
```bash
python run_pipeline.py update-status --status failed --file ids.txt
```

### 5. 生成提示词 (Generate Prompts)
生成用于模型推理的新提示词文件。
```bash
python run_pipeline.py generate --output prompts.jsonl --model deepseek-ai/DeepSeek-R1
```

### 6. 导出数据 (Export Data)
将响应导出为 JSONL，可按条件筛选。
```bash
python run_pipeline.py export --output dataset.jsonl --status passed --difficulty hard
```
