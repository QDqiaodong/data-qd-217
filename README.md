# data-qd-217 · 网络数据报分片重组

按字节偏移重组乱序分片，区分完整覆盖、缺口、终点歧义、终点外数据和冲突重叠，并保留参与分片身份。

Python 3.11+，业务运行只用标准库；构建固定 setuptools 80.9.0 与 wheel 0.45.1。

    uv venv --python 3.12 .venv
    uv pip install --python .venv/bin/python --offline -r requirements-build.lock
    uv pip install --python .venv/bin/python --offline --no-build-isolation -e .
    .venv/bin/python -m unittest discover -s tests -v
    .venv/bin/python examples/run_sample.py

样例完全合成，不包含真实网络流量。本仓不抓包、不解密、不解析上层协议，详见 docs/contract.md。

## 样本档案

`fragment_reassembly.FragmentArchive` 在 `process` 之上提供可关闭、重开的本地样本档案：更正入库（按修订号原子生效）、整套样本发布（冻结成员载荷与全部分片记录）、下游取数与审计分离（撤回后分析取数报错、审计仍读冻结旧版）。载荷正文按内容寻址共享，不为每次发布或更正复制。接口、错误与持久化边界详见 docs/archive.md，自动化测试见 tests/test_archive.py（全部使用合成数据）。
