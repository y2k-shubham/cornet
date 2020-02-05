import os
from typing import Dict, List
from unittest import TestCase

from src.main.company.drivers.task_config_reader import TaskConfigReader
from src.main.company.models.db import Db
from src.main.company.models.sqoop_step import SqoopStep
from src.main.company.models.table import Table
from src.main.company.models.task_config import TaskConfig


class TaskConfigReaderTest(TestCase):

    @staticmethod
    def _get_task_configs_folder_path() -> str:
        return "src/test/resources/task_configs"

    def test_get_task_configs(self) -> None:
        file_paths_in: List[str] = [
            os.path.join(TaskConfigReaderTest._get_task_configs_folder_path(), 'task_configs_test_1.json')
        ]

        task_configs_groups_out_expected: Dict[str, List[TaskConfig]] = {
            'instance_name': [
                TaskConfig(
                    db=Db('db_name'),
                    table=Table('table_name_1'),
                    sqoop_step=SqoopStep()
                ),
                TaskConfig(
                    db=Db('db_name'),
                    table=Table('table_name_2'),
                    sqoop_step=SqoopStep(
                        select_columns=[
                            'column_name_1',
                            'column_name_2'
                        ],
                        drop_selected=False
                    )
                ),
                TaskConfig(
                    db=Db('db_name'),
                    table=Table('table_name_2'),
                    sqoop_step=SqoopStep(
                        select_columns=[
                            'column_name_3',
                            'column_name_4'
                        ],
                        drop_selected=True
                    )
                ),
                TaskConfig(
                    db=Db('db_name'),
                    table=Table('table_name_2'),
                    sqoop_step=SqoopStep(
                        map_columns_hive={
                            'column_name_5': 'string'
                        }
                    )
                )
            ]
        }
        task_configs_groups_out_computed: Dict[str, List[TaskConfig]] = TaskConfigReader.get_task_configs(file_paths_in)

        for i in range(5):
            print(task_configs_groups_out_expected['intance_name'][i])
            print(task_configs_groups_out_computed['intance_name'][i])
            print('-----')
            self.assertEqual(task_configs_groups_out_expected['intance_name'][i], task_configs_groups_out_computed['intance_name'][i])

        #print(task_configs_groups_out_expected)
        #print(task_configs_groups_out_computed)

        self.assertDictEqual(task_configs_groups_out_expected, task_configs_groups_out_computed)
