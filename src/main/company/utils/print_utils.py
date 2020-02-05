from typing import Dict, Optional, Any

separator_length: int = 50
separators: Dict[str, str] = {
    'X': 'X' * separator_length,
    '*': '*' * separator_length,
    '-': '-' * separator_length,
    ':': ':' * separator_length,
    '.': '.' * separator_length
}


def print_with_new_lines(
        value: Any,
        caption: Optional[str] = None,
        new_line_before: bool = True,
        new_line_after: bool = True) -> None:
    if new_line_before:
        print()

    value_str: str = str(value)
    if caption:
        if '\n' in value_str:
            print(caption)
            print(value_str)
        else:
            print('{}: {}'.format(caption, value_str))
    else:
        print(value_str)

    if new_line_after:
        print()
