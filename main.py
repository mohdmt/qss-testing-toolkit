from func.kafka import kafka_get, kafka_put
from func.processor import process_kafka_download, extract_meeting_options
from func.io import get_rewind_count, display_options


# get rewind count from user
rewind_count = get_rewind_count()

# get data from wh-call-qss-
downloaded_data = kafka_get(rewind_count)

# process kafka data
processed_kafka_data = process_kafka_download(downloaded_data)
meeting_options = extract_meeting_options(processed_kafka_data)

# present options with stats
choice = display_options(meeting_options)

# poke data at intervals
kafka_put(processed_kafka_data, meeting_options[choice][0])