from func.kafka import kafka_put
from func.processor import process_kafka_download, extract_meeting_options
from func.io import display_options, get_dump_file_name


# Get name of dump file in ./temp/
filename = get_dump_file_name()

processed_kafka_data = None
with open(f'temp/{filename}.txt', 'r') as r:
    processed_kafka_data = process_kafka_download(r.read())

meeting_options = extract_meeting_options(processed_kafka_data)

# present options with stats
choice = display_options(meeting_options)

# poke data at intervals
kafka_put(processed_kafka_data, meeting_options[choice][0])