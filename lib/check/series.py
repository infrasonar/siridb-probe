from libprobe.asset import Asset
from ..connector import get_conn


async def check_series(
        asset: Asset,
        asset_config: dict,
        config: dict) -> dict:
    conn = await get_conn(asset_config, asset.name)

    query = 'count series where type == string'
    num_series_string = (await conn.query(query))['series']

    query = 'count series length where type == string'
    num_points_string = (await conn.query(query))['series_length']

    query = 'count series where type == integer'
    num_series_integer = (await conn.query(query))['series']

    query = 'count series length where type == integer'
    num_points_integer = (await conn.query(query))['series_length']

    query = 'count series where type == float'
    num_series_float = (await conn.query(query))['series']

    query = 'count series length where type == float'
    num_points_float = (await conn.query(query))['series_length']

    num_series = num_series_string + num_series_integer + num_series_float
    num_points = num_points_string + num_points_integer + num_points_float

    items = [{
        'name': 'series',
        'num_series': num_series,
        'num_points': num_points,
        'num_series_string': num_series_string,
        'num_points_string': num_points_string,
        'num_series_integer': num_series_integer,
        'num_points_integer': num_points_integer,
        'num_series_float': num_series_float,
        'num_points_float': num_points_float,
    }]

    state = {
        'series': items,
    }

    return state
