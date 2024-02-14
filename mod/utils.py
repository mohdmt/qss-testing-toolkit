import subprocess as _subprocess
from collections import defaultdict as _collections_defaultdict
from json import dumps as _json_dumps
from json import loads as _json_loads
from sys import argv as _sys_argv
from time import sleep as _time_sleep
from functools import reduce as _functools_reduce

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


def parse_args() -> tuple[bool, bool, bool, str]:
    argc = len(_sys_argv)

    # parse is_spaced
    is_described = False
    is_spaced = False
    if _sys_argv[-1] == '-d':
        _sys_argv.pop()
        argc -= 1
        is_described = True
    elif _sys_argv[-1] == '-s':
        _sys_argv.pop()
        argc -= 1
        is_spaced = True

    # validate rest of args
    if argc < 2 or argc > 3 or (_sys_argv[1] != 'LOCAL' and _sys_argv[1] != 'NONLOCAL'):
        print("USAGE: ./run.py (LOCAL|NONLOCAL) [dump_file_name] [-s|-d]")
        raise ValueError

    # parse dump file name
    dump_file_name = 'temp/dump.txt'
    if argc == 3:
        dump_file_name = _sys_argv.pop()
        argc -= 1

    # parse locality
    is_local = _sys_argv[1] == 'LOCAL'

    return is_local, is_spaced, is_described, dump_file_name


def local_get(filename: str) -> str:
    local_data = None
    with open(filename, 'r') as r:
        local_data = r.read()
    return local_data


def parse_message(ev_type: str, message: dict[str, object], user_dict: dict[str, str]) -> str:
    participant_num = user_dict[message.get(
        'participant', {}).get('participant_uuid', None)]
    match ev_type:
        case 'meeting.started':
            return f'Meeting Start'
        case 'meeting.ended':
            return f'Meeting End'
        case 'meeting.participant_joined':
            return f'Participant {participant_num} joined'
        case 'meeting.participant_left':
            return f'Participant {participant_num} left'
        case 'meeting.participant_qos':
            dt = ", ".join(set([qos.get('date_time') for qos in message.get(
                'participant', {}).get('qos', [])]))
            return f'Participant {participant_num} qos for minute(s) {dt}'
        case 'meeting.participant_data':
            data_recieved = set(_functools_reduce(lambda x, acc: acc.update(
                x), message.get('participant', {}).get('data', []), dict()).keys())
            mac_gotten = 'mac_addr' in data_recieved
            return f'Participant {participant_num} got data' + 'with MAC' if mac_gotten else ''
        case 'meeting.participant_feedback':
            return f'Participant {participant_num} gave feedback'
        case _:
            return 'some useless stuff'


def filter_dataset(*, dataset: list[dict[str, object]], filter_value: str, filter_key: str):
    return list(filter(lambda x: x.get(filter_key, '') == filter_value, dataset))
