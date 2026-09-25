"""命令行入口：python3 -m service_09251_009 --db PATH --host H --port P。

数据库路径优先取 --db，其次环境变量 CHARGE_SUB_DB，缺省落到系统临时目录，
绝不写入源码目录。运营令牌取 --operator-token 或环境变量 CHARGE_SUB_OP_TOKEN。
"""
from __future__ import annotations

import argparse
import os
import secrets
import tempfile
from pathlib import Path

from .api.http_api import create_server
from .app import build_service


def default_db_path() -> Path:
    return Path(tempfile.gettempdir()) / "charge_sub_09251" / "service.db"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="跨平台充电状态订阅交付服务")
    parser.add_argument("--db", default=os.environ.get("CHARGE_SUB_DB"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument(
        "--operator-token",
        default=os.environ.get("CHARGE_SUB_OP_TOKEN"),
        help="缺省为每次启动随机生成并打印到日志",
    )
    parser.add_argument("--lease-ttl", type=float, default=30.0)
    args = parser.parse_args(argv)

    db_path = Path(args.db) if args.db else default_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    token = args.operator_token or secrets.token_urlsafe(24)

    service, _store = build_service(db_path, lease_ttl=args.lease_ttl)
    httpd, _api = create_server(service, token, args.host, args.port)
    print(f"订阅交付服务启动: http://{args.host}:{args.port}")
    print(f"数据库: {db_path}")
    if not args.operator_token:
        print(f"本次运营令牌（CHARGE_SUB_OP_TOKEN）: {token}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":  # pragma: no cover
    main()
