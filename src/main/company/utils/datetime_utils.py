from datetime import timedelta
from typing import Tuple, Optional


def get_duration(
        duration: timedelta,
        get_hours: bool = False,
        get_minutes: bool = True) -> Tuple[Optional[int], Optional[int], int]:
    if get_hours:
        hours, remainder = divmod(duration.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        return (int(hours), int(minutes), int(seconds))
    elif get_minutes:
        minutes, seconds = divmod(duration.total_seconds(), 60)
        return (None, int(minutes), int(seconds))
    else:
        return (None, None, int(duration.total_seconds()))


def get_duration_str(
        duration: timedelta,
        get_hours: bool = False,
        get_minutes: bool = True) -> str:
    hours, minutes, seconds = get_duration(duration, get_hours, get_minutes)
    if get_hours:
        format_str: str = '{hrs} hrs, {min} min, {sec} sec'
    elif get_minutes:
        format_str: str = '{min} min, {sec} sec'
    else:
        format_str: str = '{sec} sec'
    duration_str: str = format_str.format(hrs=hours, min=minutes, sec=seconds)
    return duration_str
