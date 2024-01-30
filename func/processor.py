from json import loads as _json_loads
from collections import defaultdict as _collections_defaultdict


def process_kafka_download(input_data: str) -> list[dict[str, object]]:
    current_offset = None
    buffer = []
    recording = False
    processed_list = []
    for line in input_data.split('\n'):
        if len(line) == 1 and line[0] == '}':
            buffer.append("}")
            processed_list.append(_json_loads("".join(buffer)))
            processed_list[-1]['offset'] = current_offset
            recording = False
        if len(line) == 1 and line[0] == '{':
            buffer.clear()
            buffer.append("{")
            recording = True
        elif recording:
            buffer.append(line)
        elif line.startswith("# Topic/Partition/Offset: wh-call-qss-staging/0/"):
            current_offset = int(line.split('/')[-1])

    return processed_list

def extract_meeting_options(data: list[dict[str, object]]) -> list[tuple[str, int]]:
    meeting_object_counts = _collections_defaultdict(lambda: (0, float("inf")))
    for obj in data:
        meeting_uuid = obj['payload']['meeting_uuid']
        offset = obj.pop('offset')

        current_data = meeting_object_counts[meeting_uuid]

        meeting_object_counts[meeting_uuid] = (current_data[0] + 1, min(current_data[1], offset))

    flat_list = list(map(lambda x: (x[0], x[1][0], x[1][1]), meeting_object_counts.items()))
    return list(map(lambda x: (x[0], x[1]), sorted(flat_list, key=lambda x: x[2])))