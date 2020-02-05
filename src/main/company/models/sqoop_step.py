from typing import List, Dict, Optional, Any

"""
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
  }
"""


class SqoopStep:

    def __init__(
            self,
            select_columns: Optional[List[str]] = None,
            drop_selected: bool = True,
            map_columns_java: Optional[Dict[str, str]] = None,
            map_columns_hive: Optional[Dict[str, str]] = None) -> None:
        self.select_columns: Optional[List[str]] = select_columns
        self.drop_selected: bool = drop_selected
        self.map_columns_java: Optional[Dict[str, str]] = map_columns_java
        self.map_columns_hive: Optional[Dict[str, str]] = map_columns_hive

    def __repr__(self) -> str:
        repr: str = """
            SqoopStep(
                select_columns={select_columns},
                drop_selected={drop_selected},
                map_columns_java={map_columns_java},
                map_columns_hive={map_columns_hive}
            )
        """.format(
            select_columns=self.select_columns,
            drop_selected=self.drop_selected,
            map_columns_java=self.map_columns_java,
            map_columns_hive=self.map_columns_hive
        )
        return repr

    @staticmethod
    def load(my_dict: Dict[str, Any]) -> 'SqoopStep':
        return SqoopStep(**my_dict)
