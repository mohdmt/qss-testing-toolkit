from .io import display_message_options
from .utils import create_defaultdict, deepcopy_object, json_loads

last_ts = None


def process_kafka_download(input_data: str) -> list[dict[str, object]]:
    current_offset = None
    buffer = []
    recording = False
    processed_list = []
    for line in input_data.split('\n'):
        if len(line) == 1 and line[0] == '}':
            buffer.append("}")
            processed_list.append(json_loads("".join(buffer)))
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

    double_processed_list = []
    for obj in processed_list:
        data_as_dict = json_loads(obj['payload']['data'])
        obj_copy = deepcopy_object(obj)
        obj_copy['payload']['data'] = data_as_dict
        double_processed_list.append(obj_copy)

    return double_processed_list


def extract_meeting_options(data: list[dict[str, object]]) -> list[tuple[str, int]]:
    meeting_object_counts = create_defaultdict(lambda: (0, float("inf")))
    for obj in data:
        meeting_uuid = obj['payload']['meeting_uuid']
        offset = obj.pop('offset')

        current_data = meeting_object_counts[meeting_uuid]

        meeting_object_counts[meeting_uuid] = (
            current_data[0] + 1, min(current_data[1], offset))

    flat_list = list(
        map(lambda x: (x[0], x[1][0], x[1][1]), meeting_object_counts.items()))
    return list(map(lambda x: (x[0], x[1]), sorted(flat_list, key=lambda x: x[2])))


def create_user_dict(*, meeting: list[dict[str, object]]):
    filtered_by_events = filter(
        lambda x: x['payload']['event'] == 'meeting.participant_data', meeting)
    flattened_participants = map(
        lambda x: x['payload']['data']['participant']['participant_id'], filtered_by_events)
    participant_id_seen = set()
    unique_participant_ids = [participant_id for participant_id in flattened_participants if not (
        participant_id in participant_id_seen or participant_id_seen.add(participant_id))]
    return {participant_id: i for i, participant_id in enumerate(unique_participant_ids)}


def get_next_idx(*, target_meeting: list[dict[str, object]], user_dict: dict[str, str], is_described: bool = False):
    if target_meeting is None or len(target_meeting) == 0 or user_dict is None:
        return None

    if not is_described:
        return 0
    else:
        return display_message_options(target_meeting, user_dict)


def get_sleep_interval(*, next_ts: int, speedup_factor: int, is_spaced: bool, is_described: bool):
    global last_ts
    if is_spaced:
        input("PRESS ENTER TO SEND A NEW MESSAGE: ")
        return 0
    elif is_described:
        return 0

    sleep_duration = int(max(next_ts - last_ts, 1) /
                         max(speedup_factor, 0.000000001))
    last_ts = next_ts
    return sleep_duration
