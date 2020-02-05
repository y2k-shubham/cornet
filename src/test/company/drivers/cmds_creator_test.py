from typing import Dict
from unittest import TestCase

from src.main.company.drivers.cmds_creator import CmdsCreator
from src.main.company.models.db import Db
from src.main.company.models.hive_join_step import HiveJoinStep
from src.main.company.models.hive_orc_step import HiveOrcStep
from src.main.company.models.sqoop_step import SqoopStep
from src.main.company.models.table import Table
from src.main.company.models.task_config import TaskConfig
from src.main.company.utils.process_utils import process_cmds_dict


class CmdsCreatorTest(TestCase):

    def test_get_cmds_dict_1(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promos'),
            sqoop_step=SqoopStep()
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        # process_cmds_dict(cmds_dict)

    def test_get_cmds_dict_2(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promos'),
            sqoop_step=SqoopStep(
                select_columns=[
                    'promo_id',
                    'name',
                    'start_datetime',
                    'end_datetime',
                    'is_active'
                ],
                drop_selected=False
            )
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        # process_cmds_dict(cmds_dict)

    def test_get_cmds_dict_3(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promo_codes'),
            sqoop_step=SqoopStep(
                select_columns=[
                    'updated_at',
                    'updated_by'
                ],
                drop_selected=True
            )
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        # process_cmds_dict(cmds_dict)

    def test_get_cmds_dict_4(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promos'),
            sqoop_step=SqoopStep(
                map_columns_java={
                    'updated_at': 'String'
                }
            )
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        # process_cmds_dict(cmds_dict)

    def test_get_cmds_dict_5(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promo_codes'),
            sqoop_step=SqoopStep(
                map_columns_hive={
                    'updated_at': 'timestamp',
                    'is_active': 'string'
                }
            )
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        # process_cmds_dict(cmds_dict)

    def test_get_cmds_dict_6(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promos'),
            sqoop_step=SqoopStep(
                map_columns_java={
                    "updated_at": "String",
                    "priority": "short"
                },
                map_columns_hive={
                    "service": "string",
                    "is_active": "boolean"
                }
            )
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        # process_cmds_dict(cmds_dict)

    def test_get_cmds_dict_7(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promos'),
            sqoop_step=SqoopStep(),
            hive_orc_step=HiveOrcStep()
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        # process_cmds_dict(cmds_dict)

    def test_get_cmds_dict_8(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promo_codes'),
            sqoop_step=SqoopStep(
                select_columns=[
                    'updated_at',
                    'updated_by'
                ],
                drop_selected=False
            ),
            hive_orc_step=HiveOrcStep()
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        # process_cmds_dict(cmds_dict)

    def test_get_cmds_dict_9(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promos'),
            sqoop_step=SqoopStep(),
            hive_orc_step=HiveOrcStep(
                select_columns=[
                    'promo_id',
                    'name',
                    'start_datetime',
                    'end_datetime',
                    'is_active'
                ],
                drop_selected=False
            )
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        # process_cmds_dict(cmds_dict)

    def test_get_cmds_dict_10(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promo_codes'),
            sqoop_step=SqoopStep(),
            hive_orc_step=HiveOrcStep(
                select_columns=[
                    'updated_at',
                    'updated_by'
                ],
                drop_selected=True
            )
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        # process_cmds_dict(cmds_dict)

    def test_get_cmds_dict_11(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promo_codes'),
            sqoop_step=SqoopStep(),
            hive_orc_step=HiveOrcStep(
                add_columns=[
                    {
                        'name': 'dt',
                        'type': 'string',
                        'select_stmt': 'date_format(`updated_at`, \'yyyyMMdd\')'
                    }
                ]
            )
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        # process_cmds_dict(cmds_dict)

    def test_get_cmds_dict_12(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promo_codes'),
            sqoop_step=SqoopStep(),
            hive_orc_step=HiveOrcStep(
                partition={
                    'name': 'dt',
                    'type': 'string',
                    'select_stmt': 'date_format(`updated_at`, \'yyyyMMdd\')'
                }
            )
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        # process_cmds_dict(cmds_dict)

    def test_get_cmds_dict_13(self) -> None:
        task_config_in: TaskConfig = TaskConfig(
            db=Db('companypromodb'),
            table=Table('promo_codes'),
            sqoop_step=SqoopStep(),
            hive_orc_step=HiveOrcStep(),
            hive_join_step=HiveJoinStep(
                with_table='promos',
                on_column='promo_id',
                using_column='promo_id',
                partition={
                    'name': 'dt',
                    'type': 'string',
                    'select_stmt': 'date_format(`created_at`, \'yyyyMMdd\')'
                }
            )
        )
        cmds_dict: Dict[int, Dict[str, str]] = CmdsCreator.get_cmds_dict(task_config_in)
        process_cmds_dict(cmds_dict)
