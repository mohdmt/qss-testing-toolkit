from .utils import run_subprocess, get_epoch_from_ts, sleep_for

def kafka_get(count: int) -> str:
    return run_subprocess("kafkaclient", "-env", "staging", "-topic", "wh-call-qss-", "-rewind", str(count))

def kafka_put(data: list[dict[str, object]], meeting_uuid: str):
    filtered_data = filter(lambda x: x['payload']['meeting_uuid'] == meeting_uuid, data)
    sorted_data = sorted(filtered_data, key=lambda x: get_epoch_from_ts(x['timestamp']))
    last_ts = None

    for data in sorted_data:
        if last_ts == None:
            last_ts = get_epoch_from_ts(data['timestamp'])

        current_ts = get_epoch_from_ts(data['timestamp'])
        sleep_for(current_ts - last_ts)
        last_ts = current_ts

        run_subprocess("kafkapoke", "-topic", "incoming-call-qss-", "-env", "staging", "-count", "1", "-stdin", input_data=data)

    print("\nFinished sending messages to kafka!")
