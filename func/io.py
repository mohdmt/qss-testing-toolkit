def get_rewind_count():
    return int(input("Rewind count from wh-call-qss: "))

def display_options(options_list: list[tuple[str, int, int]]) -> int:
    while True:
        print("\n\nSelect index of the meeting ID to be kafkapoke'd:")
        for idx, (meeting_uuid, object_count) in enumerate(options_list):
            print(f'{idx:02}) UUID: {meeting_uuid} Count: {object_count:03}')
        choice = int(input())
        if choice < len(options_list) and choice >= 0:
            print(f"\nSending messages from meeting UUID {options_list[choice][0]} to incoming-call-qss-")
            return choice
        print(f"Index should be between {0} and {len(options_list) - 1}")
