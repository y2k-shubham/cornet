from typing import Dict, List

from src.main.company.config.config import Config
from src.main.company.models.task_config import TaskConfig


def group_task_configs_by_instance(task_configs: List[TaskConfig]) -> Dict[str, List[TaskConfig]]:
    db_instance_map: Dict[str, str] = Config.get_db_instance_map()
    db_instances: List[str] = list(db_instance_map.values())

    grouped_task_configs: Dict[str, List[TaskConfig]] = {
        instance: sorted(filter_task_configs_by_instance(task_configs, instance))
        for instance in db_instances}

    # remove instances for which no task_configs are present
    for db_instance in db_instances:
        if db_instance in grouped_task_configs and not grouped_task_configs[db_instance]:
            grouped_task_configs.pop(db_instance)

    return grouped_task_configs


def filter_task_configs_by_instance(task_configs: List[TaskConfig], instance: str) -> List[TaskConfig]:
    db_instance_map: Dict[str, str] = Config.get_db_instance_map()
    filtered_task_configs: List[TaskConfig] = [task_config for task_config in task_configs if
                                               db_instance_map[task_config.db.name] == instance]
    return filtered_task_configs
