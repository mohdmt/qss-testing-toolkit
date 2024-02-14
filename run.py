#!/Users/mmotorwala/.pyenv/shims/python3

from mod.io import display_meeting_options, get_rewind_count, get_speedup_factor
from mod.kafka import kafka_get, kafka_put
from mod.processor import extract_meeting_options, process_kafka_download, get_next_idx, get_sleep_interval, create_user_dict
from mod.utils import local_get, parse_args, filter_dataset


def main():
    try:
        is_local, is_spaced, is_described, dump_file_name = parse_args()

        # get inputs from user (if needed) and retrieve data
        speedup_factor = 1
        if not is_spaced and not is_described:
            speedup_factor = get_speedup_factor()

        if is_local:
            downloaded_data = local_get(dump_file_name)
        else:
            rewind_count = get_rewind_count()
            downloaded_data = kafka_get(rewind_count)

        # process kafka data
        processed_kafka_data = process_kafka_download(downloaded_data)
        meeting_options = extract_meeting_options(processed_kafka_data)

        # present meeting options with stats
        meeting_choice = display_meeting_options(meeting_options)

        # only retreive indicated meeting and create user id mapping
        target_meeting = filter_dataset(
            dataset=meeting_options, filter_value=meeting_choice, filter_key='id')
        user_dict = create_user_dict(meeting=target_meeting)

        while True:
            message_idx = get_next_idx(
                target_meeting=target_meeting, user_dict=user_dict, is_described=is_described)
            if message_idx is None:
                break
            target_message = target_meeting.pop(message_idx)
            sleep_duration = get_sleep_interval(
                next_ts=target_message['timestamp'], speedup_factor=speedup_factor, is_spaced=is_spaced, is_described=is_described)
            # poke data at intervals
            kafka_put(data=target_message, sleep_duration=sleep_duration)

        print("FINISHED SENDING MESSAGES TO KAFKA!")
    except ValueError:
        return


if __name__ == "__main__":
    main()
