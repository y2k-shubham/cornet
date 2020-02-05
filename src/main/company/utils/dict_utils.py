import copy
from typing import List, Optional, Any, Dict, Tuple

from src.main.company.utils.string_utils import find_delim, split_on_first_occurrence


def merge_dict(dict_a: Dict[Any, Any], dict_b: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Merge dictionaries dict_a and dict_b recursively. If necessary, dict_a overrides dict_b .
    A new dictionary is returned, input arguments are not changed.
    """
    assert isinstance(dict_a, dict), "First arg not dict_a dict, but {0} ".format(dict_a)
    assert isinstance(dict_b, dict), "Second arg not dict_a dict, but {0} ".format(dict_b)

    combined_dict: Dict[Any, Any] = {**dict_a, **dict_b}

    merged_dict: Dict[Any, Any] = {}
    for key in combined_dict.keys():
        if key not in dict_b:
            merged_dict[key]: Any = dict_a[key]
        elif key not in dict_a:
            merged_dict[key]: Any = dict_b[key]
        elif isinstance(dict_a[key], dict) and isinstance(dict_b[key], dict):
            merged_dict[key]: Dict[Any, Any] = merge_dict(dict_a[key], dict_b[key])
        elif isinstance(dict_a[key], list) and isinstance(dict_b[key], list):
            merged_dict[key]: List[Any] = list(
                merge_dict(dict.fromkeys(dict_a[key]), dict.fromkeys(dict_b[key])).keys())
        else:
            merged_dict[key]: Any = dict_a[key]

    return merged_dict


def dict_remove_key(my_dict: Dict[Any, Any], key: Any) -> Dict[Any, Any]:
    """ Returned a new copy of dictionary without a specified key """
    dict_copy: Dict[Any, Any] = copy.deepcopy(my_dict)
    if key in dict_copy:
        del dict_copy[key]
    return dict_copy


def dict_get_value_recursively(my_dict: Dict[str, Any], key: str) -> Optional[Any]:
    delim: Optional[str] = find_delim(key)

    if delim is None:
        return my_dict.get(key, None)
    else:
        current_path, remaining_path = split_on_first_occurrence(key, delim)
        if isinstance(my_dict.get(current_path, None), dict):
            return dict_get_value_recursively(my_dict[current_path], remaining_path)
        else:
            return None


def append_to_indexed_dict(my_dict: Dict[int, Any], new_items: Any) -> Dict[int, Any]:
    """
    Accepts an indexed dict (keys are 0, 1, 2, 3..) and new_items,
    (single or a list) and adds items to dictionary and returns
    the new dictionary (leaves original  dictionary unmodified)

    This is done only when new_items is truthy value, otherwise
    original indexed dictionary is returned unmodified
    :param my_dict:   Indexed dictionary in which to append items
    :type my_dict:    Dict[int, Any]
    :param new_items: item or list of items to be appended to indexed dict
    :type new_items:  Any
    :return:          Updated indexed dictionary with list of items appended to it
                      Original dictionary is left unmodified
    :type:            Dict[int, Any]
    """
    if new_items:
        max_index: int = max(list(my_dict.keys())) if my_dict else 0
        dict_copy: Dict[Any, Any] = copy.deepcopy(my_dict)

        crr_index: int = max_index
        if isinstance(new_items, List):
            for item in new_items:
                crr_index: int = crr_index + 1
                dict_copy[crr_index]: Any = item
        else:
            dict_copy[crr_index + 1]: Any = new_items

        return dict_copy
    else:
        return my_dict


def convert_to_list_of_tuples(my_dict: Dict[Any, Any]) -> List[Tuple[Any, Any]]:
    list_of_tuples: List[Any, Any] = [(key, value) for key, value in my_dict.items()]
    return list_of_tuples
