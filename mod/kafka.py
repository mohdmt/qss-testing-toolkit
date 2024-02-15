from .utils import json_dumps, run_subprocess, sleep_for


def kafka_get(count: int) -> str:
    return run_subprocess("kafkaclient", "-env", "staging", "-topic", "wh-call-qss-", "-rewind", str(count))


def kafka_put(data: dict[str, object], sleep_duration: int):
    data['payload']['data'] = json_dumps(data['payload']['data'])
    run_subprocess("kafkapoke", "-topic", "incoming-call-qss-",
                   "-env", "staging", "-count", "1", "-stdin", input_data=data)
    sleep_for(sleep_duration)
