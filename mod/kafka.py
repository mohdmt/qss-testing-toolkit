from .utils import get_epoch_from_ts, run_subprocess, sleep_for


def kafka_get(count: int) -> str:
    return run_subprocess("kafkaclient", "-env", "staging", "-topic", "wh-call-qss-", "-rewind", str(count))


def kafka_put(data: list[dict[str, object]], m_filter: str = None, speedup_factor: int = 1, is_spaced: bool = False) -> None:
    if m_filter is not None:
        data = filter(
            lambda x: x['payload']['meeting_uuid'] == m_filter, data)
    # sort the data by timestamp
    data = sorted(
        data, key=lambda x: get_epoch_from_ts(x['timestamp']))

    last_ts = None
    for i, dat in enumerate(data, start=1):
        if last_ts == None:
            last_ts = get_epoch_from_ts(dat['timestamp'])

        current_ts = get_epoch_from_ts(dat['timestamp'])

        last_ts = current_ts
        run_subprocess("kafkapoke", "-topic", "incoming-call-qss-",
                       "-env", "staging", "-count", "1", "-stdin", input_data=dat)

        # space out messages but either a timeout or input
        if is_spaced:
            print(f"PRESS ENTER TO SEND A NEW MESSAGE ({i})", end='')
            input()
        else:
            sleep_for(int(max(current_ts - last_ts, 1) / max(speedup_factor, 0.000000001)))

    print("\nFinished sending messages to kafka!")
