from .utils import clear_terminal, parse_message


def get_rewind_count():
    clear_terminal()
    return int(input("Rewind count from wh-call-qss: "))


def get_speedup_factor():
    clear_terminal()
    inp = input("Speedup factor: ")
    try:
        input_num = float(inp)
        return input_num
    except ValueError:
        return None


def display_finished_message():
    clear_terminal()
    print("FINISHED SENDING MESSAGES TO KAFKA!")


def display_usage_message():
    print("USAGE: ./run.py (LOCAL|NONLOCAL) [dump_file_name] [-s|-d]")


def get_dump_file_name():
    clear_terminal()
    return input("Enter name of kafka dump file in the temp folder: ")


def display_meeting_options(options_list: list[tuple[str, int]]) -> int:
    while True:
        clear_terminal()
        print("\n\nSelect index of the meeting ID to be kafkapoke'd:")
        for idx, (meeting_uuid, object_count) in enumerate(options_list):
            print(f'{idx:02}) UUID: {meeting_uuid} Count: {object_count:03}')
        choice = int(input())
        if choice < len(options_list) and choice >= 0:
            print(
                f"\nSending messages from meeting UUID {options_list[choice][0]} to incoming-call-qss-")
            return options_list[choice][0]
        print(f"Index should be between {0} and {len(options_list) - 1}")


def display_message_options(target_meeting: list[dict[str, object]], user_dict: dict[str, str]):
    for _ in range(100):
        print()
    clear_terminal()
    for i, message in enumerate(target_meeting):
        payload = message.get('payload', {})
        ev_type = payload.get('event', '')
        message_summary = parse_message(ev_type=ev_type,
                                        payload=payload, user_dict=user_dict)
        print(f"{i:03}) {message_summary}")
    while True:
        try:
            required = int(input('Enter required idx: '))
            if required >= 0 and required < len(target_meeting):
                return required
            else:
                raise ValueError
        except ValueError as _:
            print(
                f"Invalid idx - must be between {0} and {len(target_meeting)}")
