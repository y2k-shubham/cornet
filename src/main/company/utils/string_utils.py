import re
from typing import List, Optional, Tuple, Union

from src.main.company.models.db import Db
from src.main.company.models.table import Table


def find_delim(string: str) -> Optional[str]:
    if "/" in string:
        return "/"
    elif "." in string:
        return "."
    else:
        return None


def split_on_first_occurrence(string: str, delim: str) -> Tuple[str, Optional[str]]:
    tokens: List[str] = string.split(delim)
    first_part: str = tokens[0]
    second_part: str = delim.join(tokens[1:])
    second_part: Optional[str] = second_part if second_part else None
    return first_part, second_part


def match_any(patterns: List[str], string: str) -> bool:
    """ Return true if s matches any of the regexps"""
    return any(re.fullmatch(pattern, string) for pattern in patterns)


def merge_db_table_names(db: Union[Db, str], table: Union[Table, str]) -> str:
    db_name: str = db.name if isinstance(db, Db) else db
    table_name: str = table.name if isinstance(table, Table) else table
    return db_name + '.' + table_name


def split_db_table_names(db_table_name: str) -> Tuple[str, Optional[str]]:
    """
    Splits a given string 'db_name.table_name' into tuple ('db_name', 'table_name').
    If only one token is present (not '.' dot), it is treated as db_name
    :param db_table_name: Name to be split
    :type db_table_name: str
    :return: (db_name, table_name) or (db_name, None)
    :type: Tuple[str, Optional[str]]
    """
    return split_on_first_occurrence(db_table_name, '.')


def get_tmp_table_names(table_name: str) -> Tuple[str, str]:
    return ((table_name + '_tmp_1'), (table_name + '_tmp_2'))
