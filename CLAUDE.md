# CLAUDE.md

本文件为 Claude Code (claude.ai/code) 在此代码库中工作提供指导。

## 项目概述

这是一个为 LangGraph 提供 Redis checkpointer 功能的 Python 包。项目使用 `uv` 作为包管理器和构建工具。

## 包管理

- **包管理器**: `uv` (现代 Python 包管理器)
- **虚拟环境**: 使用 `.venv/` 目录 (已配置)
- **锁文件**: `uv.lock` 包含精确的依赖版本

## 常用命令

### 环境设置

```bash
# 安装依赖
uv install

# 激活虚拟环境
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows
```

### 包管理

```bash
# 添加新依赖
uv add <package-name>

# 添加开发依赖
uv add --dev <package-name>

# 移除依赖
uv remove <package-name>

# 更新依赖
uv update
```

### 开发命令

```bash
# 以可编辑模式安装包
uv install -e .

# 构建包
uv build

# 运行 Python 并使用包
uv run python -c "from langgraph_redis_checkpointer import hello; print(hello())"
```

## 项目结构

```
src/langgraph_redis_checkpointer/
├── __init__.py           # 主包入口点
```

## 架构说明

- 这是一个为 LangGraph 提供基于 Redis 的 checkpointer 功能的 Python 包
- 目前只包含带有 `hello()` 函数的最小启动代码
- 包遵循标准的 Python 包结构，使用 `src/` 布局
- 使用 `pyproject.toml` 进行配置 (hatchling 构建后端)

## 开发环境设置

1. 确保安装了 Python 3.11+ (如 `uv.lock` 中指定)
2. 运行 `uv install` 设置环境和依赖
3. 安装后包将可用于导入

## 重要说明

- 项目目前除了构建系统外没有外部依赖
- 尚未配置测试框架
- 尚未设置代码质量检查工具
- 这似乎是一个处于早期开发阶段的新项目
