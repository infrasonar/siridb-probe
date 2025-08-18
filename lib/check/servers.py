from libprobe.asset import Asset
from ..connector import get_conn


async def check_servers(
        asset: Asset,
        asset_config: dict,
        config: dict) -> dict:
    conn = await get_conn(asset_config, asset.name)

    query = (
        'list servers '
        'name,version,status,uptime,active_handles,active_tasks,'
        'mem_usage,idle_percentage,fifo_files')

    data = await conn.query(query)
    items = [{
        'name': server[0],
        'version': server[1],
        'status': server[2],
        'uptime': server[3],
        'active_handles': server[4],
        'active_tasks': server[5],
        'mem_usage': server[6],
        'idle_percentage': server[7],
        'fifo_files': server[8],
    } for server in data['servers']]

    state = {
        'servers': items,
    }

    return state
