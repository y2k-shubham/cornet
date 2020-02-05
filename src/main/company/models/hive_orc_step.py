from typing import List, Dict, Optional, Any

"""
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
  }
"""


class HiveOrcStep:

    def __init__(
            self,
            select_columns: Optional[List[str]] = None,
            drop_selected: bool = True,
            add_columns: Optional[List[Dict[str, str]]] = None,
            partition: Optional[Dict[str, str]] = None) -> None:
        self.select_columns: Optional[List[str]] = select_columns
        self.drop_selected: bool = drop_selected
        self.add_columns: Optional[List[Dict[str, str]]] = add_columns
        self.partition: Optional[Dict[str, str]] = partition

    def __repr__(self) -> str:
        repr: str = """
            HiveOrcStep(
                select_columns={select_columns},
                drop_selected={drop_selected},
                add_columns={add_columns},
                partition={partition}
            )
        """.format(
            select_columns=self.select_columns,
            drop_selected=self.drop_selected,
            add_columns=self.add_columns,
            partition=self.partition
        )
        return repr

    @staticmethod
    def load(my_dict: Dict[str, Any]) -> 'HiveOrcStep':
        return HiveOrcStep(**my_dict)
