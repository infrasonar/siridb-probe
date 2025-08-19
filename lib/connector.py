import asyncio
import time
from siridb.connector import SiriDBConn
from libprobe.exceptions import CheckException

_connections: dict[
    tuple[str, str, str, int],
    tuple[float, asyncio.Lock, SiriDBConn]] = {}


async def _get_conn(username: str, password: str, database: str, host: str,
                    port: int):
    key = (username, database, host, port)
    expire_ts, lock, conn = _connections.get(key, (0.0, asyncio.Lock(), None))
    if conn is None or (time.time() > expire_ts and not lock.locked()):
        if conn:
            try:
                conn.close()
                await conn.wait_closed()
            except Exception:
                pass

        conn = SiriDBConn(
            username=username,
            password=password,
            dbname=database,
            server=host,
            port=port)
        await conn.connect()
    return conn, lock


async def get_conn(asset_config: dict,
                   asset_name: str) -> tuple[SiriDBConn, asyncio.Lock]:
    host = asset_config.get('host') or asset_name
    port = asset_config.get('port', 9000)
    username = asset_config.get('username')
    password = asset_config.get('password')
    database = asset_config.get('database')

    if not isinstance(host, str) or not host:
        raise CheckException('missing or invalid `host` in asset config')
    if not isinstance(port, int) or 0 > port > 65531:
        raise CheckException('invalid `port` in asset config')
    if not isinstance(username, str) or not username:
        raise CheckException('missing or invalid `username` in asset config')
    if not isinstance(password, str) or not password:
        raise CheckException('missing or invalid `password` in asset config')
    if not isinstance(database, str) or not database:
        raise CheckException('missing or invalid `database` in asset config')

    conn, lock = await _get_conn(username, password, database, host, port)
    return conn, lock


async def close_all():
    for expire_ts, lock, conn in _connections.values():
        async with lock:
            try:
                conn.close()
                await conn.wait_closed()
            except Exception:
                pass
