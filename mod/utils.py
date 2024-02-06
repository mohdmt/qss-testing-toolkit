import subprocess as _subprocess
from collections import defaultdict as _collections_defaultdict
from json import dumps as _json_dumps
from json import loads as _json_loads
from sys import argv as _sys_argv
from time import sleep as _time_sleep

from dateutil.parser import parse as _parse_dt


def run_subprocess(*args, input_data: str = None) -> list[str]:
    output = None
    if input_data is None:
        output = _subprocess.check_output(args)
    else:
        process = _subprocess.Popen(args,
                                    stdin=_subprocess.PIPE,
                                    stdout=_subprocess.PIPE,
                                    stderr=_subprocess.PIPE)
        output, _ = process.communicate(
            input=_json_dumps(input_data).encode("utf-8"))

    return output.decode("utf-8") if output is not None else None


def date_subtract(datetime1: str, datetime2: str) -> int:
    return get_epoch_from_ts(datetime2) - get_epoch_from_ts(datetime1)


def get_epoch_from_ts(datetime: str) -> int:
    return int(_parse_dt(datetime).timestamp())


def sleep_for(secs: int) -> None:
    _time_sleep(secs)


def json_loads(input: str) -> dict[str, object]:
    return _json_loads(input)


def create_defaultdict(func) -> _collections_defaultdict:
    return _collections_defaultdict(func)


def parse_args() -> tuple[bool, str]:
    argc = len(_sys_argv)
    if argc < 2 or argc > 3 or (_sys_argv[1] != 'LOCAL' and _sys_argv[1] != 'NONLOCAL'):
        print("USAGE: ./run.py (LOCAL|NONLOCAL) [dump_file_name]")
        raise ValueError

    return _sys_argv[1] == 'LOCAL', 'temp/dump.txt' if argc == 2 else _sys_argv[2]


def local_get(filename: str) -> str:
    local_data = None
    with open(filename, 'r') as r:
        local_data = r.read()
    return local_data
