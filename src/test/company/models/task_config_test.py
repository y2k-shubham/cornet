from unittest import TestCase

from src.main.company.models.db import Db
from src.main.company.models.sqoop_step import SqoopStep
from src.main.company.models.table import Table
from src.main.company.models.task_config import TaskConfig


class TaskConfigTest(TestCase):

    @staticmethod
    def _get_task_configs_folder_path() -> str:
        return "src/test/resources/task_configs"

    def test__init__(self) -> None:
        task_config: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promos'),
            sqoop_step=SqoopStep(
                map_columns_hive={
                    'service': 'string',
                    'is_active': 'boolean'
                }
            )
        )
        print(task_config.sqoop_step.map_columns_hive)
