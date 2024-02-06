#!/Users/mmotorwala/.pyenv/shims/python3

from mod.io import (display_options, get_rewind_count,get_speedup_factor)
from mod.kafka import kafka_get, kafka_put
from mod.processor import extract_meeting_options, process_kafka_download
from mod.utils import local_get, parse_args


def main():
    try:
        is_local, dump_file_name = parse_args()

        # get inputs from user (if needed) and retrieve data
        speedup_factor = get_speedup_factor()
        if is_local:
            downloaded_data = local_get(dump_file_name)
        else:
            rewind_count = get_rewind_count()
            downloaded_data = kafka_get(rewind_count)

        # process kafka data
        processed_kafka_data = process_kafka_download(downloaded_data)
        meeting_options = extract_meeting_options(processed_kafka_data)

        # present options with stats
        choice = display_options(meeting_options)

        # poke data at intervals
        kafka_put(processed_kafka_data, meeting_options[choice][0], speedup_factor)
    except ValueError:
        return


if __name__ == "__main__":
    main()