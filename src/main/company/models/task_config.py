from typing import Optional, Dict, Any

from src.main.company.config.config import Config
from src.main.company.models.db import Db
from src.main.company.models.hive_join_step import HiveJoinStep
from src.main.company.models.hive_orc_step import HiveOrcStep
from src.main.company.models.sqoop_step import SqoopStep
from src.main.company.models.table import Table

"""
  "task_config": {
    "db": {
        "name": "db_name_1"
    },
    "table": {
        "name": "table_name_1"
    },
    "sqoop_step": {
        "select_columns": [
            "column_name_1",
            "column_name_2",
            "column_name_3"
        ],
        "drop_selected": true,
        "map_columns_java": {
            "column_name_1": "String",
            "column_name_2": "long"
        },
        "map_columns_hive": {
            "column_name_3": "bigint",
            "column_name_4": "boolean"
        }
    },
    "hive_orc_step": {
        "select_columns": [
            "column_name_1",
            "column_name_2",
            "column_name_3"
        ],
        "drop_selected": true,
        "add_columns": [
            {
                "name": "new_column_name_1",
                "type": "tinyint",
                "select_stmt": "CASE WHEN \\`email\\` LIKE '%@company.com' THEN 1 ELSE 0 END"
            }
        ],
        "partition": {
            "name": "my_column_1",
            "type": "string",
            "select_stmt": "DATE_FORMAT(\\`my_column_1\\` AS 'yyyyMMdd')"
        }
    },
    "hive_join_step": {
        "with_table": "my_table",
        "on_column": "some_id_column",
        "using_column": "corresponding_id_column",
        "partition": {
          "name": "dt",
          "type": "string",
          "select_stmt": "date_format(\\`created_on\\`, 'yyyyMMdd')"
        }
    }
  }
"""


class TaskConfig:

    def __init__(
            self,
            db: Db,
            table: Table,
            sqoop_step: SqoopStep,
            hive_orc_step: Optional[HiveOrcStep] = None,
            hive_join_step: Optional[HiveJoinStep] = None) -> None:
        self.db: Db = db
        self.table: Table = table
        self.sqoop_step: SqoopStep = sqoop_step
        self.hive_orc_step: Optional[HiveOrcStep] = hive_orc_step
        self.hive_join_step: Optional[HiveJoinStep] = hive_join_step

    def __repr__(self) -> str:
        repr: str = """
            TaskConfig(
                db=%r,
                table=%r,
                sqoop_step=%r,
                hive_orc_step=%r,
                hive_join_step=%r
            )
        """ % (
            self.db,
            self.table,
            self.sqoop_step,
            self.hive_orc_step,
            self.hive_join_step
        )
        return repr

    def __gt__(self, other: 'TaskConfig') -> bool:
        if self.db.name != other.db.name:
            return Config.get_db_rank(self.db.name) < Config.get_db_rank(other.db.name)
        elif not (self.is_orc() and other.is_orc()):
            return (self.is_orc() and (not other.is_orc()))
        elif not (self.is_partitioned() and other.is_partitioned()):
            return (self.is_partitioned() and (not other.is_partitioned()))
        elif not (self.is_joined() and other.is_joined()):
            return (self.is_joined() and (not other.is_joined()))
        else:
            return self.table.name > other.table.name

    def is_orc(self) -> bool:
        return self.hive_orc_step is not None

    def is_joined(self) -> bool:
        return self.hive_join_step is not None

    def is_partitioned(self) -> bool:
        return (self.is_orc() and self.hive_orc_step.partition is not None) or self.is_joined()

    @staticmethod
    def load(my_dict: Dict[str, Any]) -> 'TaskConfig':
        sqoop_step: SqoopStep = SqoopStep.load(my_dict['sqoop_step']) if 'sqoop_step' in my_dict else SqoopStep()
        hive_orc_step: Optional[HiveOrcStep] = HiveOrcStep.load(
            my_dict['hive_orc_step']) if 'hive_orc_step' in my_dict else None
        hive_join_step: Optional[HiveJoinStep] = HiveJoinStep.load(
            my_dict['hive_join_step']) if 'hive_join_step' in my_dict else None

        return TaskConfig(
            db=Db.load(my_dict['db']),
            table=Table.load(my_dict['table']),
            sqoop_step=sqoop_step,
            hive_orc_step=hive_orc_step,
            hive_join_step=hive_join_step
        )
