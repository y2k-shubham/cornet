import json
from typing import Dict, List, Any, Union

from src.main.company.models.task_config import TaskConfig
from src.main.company.utils.task_config_utils import group_task_configs_by_instance


class TaskConfigReader:

    @staticmethod
    def get_task_configs(file_paths: List[str]) -> Dict[str, List[TaskConfig]]:
        task_configs_all: List[TaskConfig] = []
        for file_path in file_paths:
            with open(file_path) as json_file:
                json_data: Union[Dict[str, Any], List[Dict[str, Any]]] = json.load(json_file)
                if isinstance(json_data, dict):
                    json_data: List[Dict[str, Any]] = [json_data]

                task_configs_current_file: List[TaskConfig] = list(
                    map(lambda json_dict: TaskConfig.load(json_dict), json_data))
                task_configs_all.extend(task_configs_current_file)

        return group_task_configs_by_instance(task_configs_all)
