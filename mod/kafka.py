from .processor import currentify_timestamps
from .utils import (date_add, date_diff, get_current_time_utc, json_dumps,
                    run_subprocess, sleep_for)


def kafka_get(count: int) -> str:
    return run_subprocess("kafkaclient", "-env", "staging", "-topic", "wh-call-qss-", "-rewind", str(count))


def kafka_put(data: dict[str, object], sleep_duration: int):
    # make all dates/timestamps current
    currentify_timestamps(data)

    # re-marshal json
    data['payload']['data'] = json_dumps(data['payload']['data'])

    # post data to kafka
    run_subprocess("kafkapoke", "-topic", "incoming-call-qss-",
                   "-env", "staging", "-count", "1", "-stdin", input_data=data)
    sleep_for(sleep_duration)
