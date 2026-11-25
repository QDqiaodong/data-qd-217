# data-qd-217 · 网络数据报分片重组

按字节偏移重组乱序分片，区分完整覆盖、缺口、终点歧义、终点外数据和冲突重叠，并保留参与分片身份。

Python 3.11+，业务运行只用标准库；构建固定 setuptools 80.9.0 与 wheel 0.45.1。

    uv venv --python 3.12 .venv
    uv pip install --python .venv/bin/python --offline -r requirements-build.lock
    uv pip install --python .venv/bin/python --offline --no-build-isolation -e .
    .venv/bin/python -m unittest discover -s tests -v
    .venv/bin/python examples/run_sample.py

样例完全合成，不包含真实网络流量。本仓不抓包、不解密、不解析上层协议，详见 docs/contract.md。
