"""启动订阅交付服务。

运行数据默认写入系统临时目录，不污染源码目录；
可用 --db 或环境变量 SVC_DB_PATH 指定 SQLite 路径，
--admin-token 或 SVC_ADMIN_TOKEN 指定运营令牌。
"""
from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

from .api import make_server
from .service import SubscriptionService
from .storage import Storage


def main() -> None:
    default_db = str(Path(tempfile.gettempdir()) / "service_09251_009" / "subscription.db")
    parser = argparse.ArgumentParser(description="跨平台充电状态订阅交付服务")
    parser.add_argument("--db", default=os.environ.get("SVC_DB_PATH") or default_db,
                        help="SQLite 数据库路径")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--admin-token",
                        default=os.environ.get("SVC_ADMIN_TOKEN", ""),
                        help="运营接口令牌（默认从 SVC_ADMIN_TOKEN 读取）")
    args = parser.parse_args()

    if not args.admin_token:
        parser.error("必须通过 --admin-token 或 SVC_ADMIN_TOKEN 提供运营令牌")

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    service = SubscriptionService(Storage(args.db))
    server = make_server(service, args.admin_token, args.host, args.port)
    host, port = server.server_address[:2]
    print(f"订阅交付服务已启动: http://{host}:{port} (db={args.db})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
