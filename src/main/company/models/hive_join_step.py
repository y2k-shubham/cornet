from typing import Dict, Any

"""
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
"""


class HiveJoinStep:

    def __init__(
            self,
            with_table: str,
            on_column: str,
            using_column: str,
            partition: Dict[str, str]) -> None:
        self.with_table: str = with_table
        self.on_column: str = on_column
        self.using_column: str = using_column
        self.partition: Dict[str, str] = partition

    def __repr__(self) -> str:
        repr: str = """
            HiveJoinStep(
                with_table={with_table},
                on_column={on_column},
                using_column={using_column},
                partition={partition}
            )
        """.format(
            with_table=self.with_table,
            on_column=self.on_column,
            using_column=self.using_column,
            partition=self.partition
        )
        return repr

    @staticmethod
    def load(my_dict: Dict[str, Any]) -> 'HiveJoinStep':
        return HiveJoinStep(**my_dict)
