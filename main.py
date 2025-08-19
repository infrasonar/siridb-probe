from libprobe.probe import Probe
from lib.check.servers import check_servers
from lib.check.series import check_series
from lib.connector import close_all
from lib.version import __version__ as version


if __name__ == '__main__':
    checks = {
        'servers': check_servers,
        'series': check_series,
    }

    probe = Probe("siridb", version, checks)
    probe.start()
    assert probe.loop
    probe.loop.run_until_complete(close_all())
