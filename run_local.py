#!/Users/mmotorwala/.pyenv/shims/python3

from mod.io import display_options, get_dump_file_name, get_speedup_factor
from mod.kafka import kafka_put
from mod.processor import extract_meeting_options, process_kafka_download

# Get name of dump file in ./temp/
filename = get_dump_file_name()
speedup_factor = get_speedup_factor()

processed_kafka_data = None
with open(f'temp/{filename}.txt', 'r') as r:
    processed_kafka_data = process_kafka_download(r.read())

meeting_options = extract_meeting_options(processed_kafka_data)

# present options with stats
choice = display_options(meeting_options)

# poke data at intervals
kafka_put(processed_kafka_data, meeting_options[choice][0], speedup_factor)
