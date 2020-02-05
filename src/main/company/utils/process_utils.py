import subprocess
from typing import Dict, List, Optional, Tuple

from src.main.company.drivers.cmds_creator import CmdsCreator
from src.main.company.models.task_config import TaskConfig
from src.main.company.utils.print_utils import separators, print_with_new_lines


def process_cmd(
        cmd_name: str,
        cmd: str,
        show: bool = True,
        execute: bool = False) -> None:
    if show:
        print_with_new_lines(value=cmd_name, new_line_after=False)
        print_with_new_lines(cmd)

    if execute:
        # https://stackoverflow.com/a/23421133/3679900
        try:
            subprocess.check_output(args=cmd, shell=True)
        except subprocess.CalledProcessError as grepexc:
            raise Exception(grepexc.output)


def process_cmds_dict(
        cmds_dict: Dict[int, Dict[str, str]],
        show: bool = True,
        execute: bool = False) -> Optional[Tuple[str, Exception]]:
    for my_ind, my_dict in cmds_dict.items():
        if show:
            print_with_new_lines(separators[':'], new_line_after=False)
            print_with_new_lines(value=my_ind, caption='Phase')
            print_with_new_lines(separators[':'], new_line_before=False)

        for cmd_name, cmd in my_dict.items():
            try:
                process_cmd(cmd_name, cmd, show, execute)
            except Exception as e:
                if my_ind == 1:
                    # error in phase 1 is not catastrophic since
                    # it is only the cleanup phase
                    print_with_new_lines(e, caption='Encountered exception in Phase-1')
                else:
                    return (cmd_name, e)
            finally:
                if show:
                    print_with_new_lines(separators['.'])


def process_task_config(
        task_config: TaskConfig,
        show: bool = True,
        execute: bool = False) -> Optional[Tuple[str, Exception]]:
    if show:
        print_with_new_lines(separators['-'], new_line_after=False)
        print_with_new_lines(task_config)
        print_with_new_lines(separators['-'], new_line_before=False)

    cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config)
    return process_cmds_dict(cmds_dict, show, execute)


def process_task_config_groups(
        task_config_groups: Dict[str, List[TaskConfig]],
        generate_cmds: bool = True,
        execute: bool = False) -> None:
    for instance, task_configs in task_config_groups.items():
        print_with_new_lines(separators['*'], new_line_after=False)
        print_with_new_lines(instance, caption='MySQL instance')
        print_with_new_lines(separators['*'], new_line_before=False)

        for task_config in task_configs:
            try:
                process_task_config(task_config, execute)
            except Exception as e:
                print_with_new_lines(e, caption='Encountered exception')
