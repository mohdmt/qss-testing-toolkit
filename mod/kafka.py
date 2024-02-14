from .utils import get_epoch_from_ts, run_subprocess, sleep_for


def kafka_get(count: int) -> str:
    return run_subprocess("kafkaclient", "-env", "staging", "-topic", "wh-call-qss-", "-rewind", str(count))


def kafka_put(data: dict[str, object], sleep_duration: int):
    run_subprocess("kafkapoke", "-topic", "incoming-call-qss-",
                   "-env", "staging", "-count", "1", "-stdin", input_data=data)
    sleep_for(sleep_duration)
