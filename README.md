打包项目并推送到私有镜像仓库
uv build
python -m twine upload --repository-url http://192.168.200.156:8081/repository/pypi-internal/ dist/\*

使用 pyproject.toml 中配置
[[tool.uv.index]]
url = "http://192.168.200.156:8081/repository/pypi-all/simple"
default = true

项目引用：
uv add langgraph_redis_checkpointer
