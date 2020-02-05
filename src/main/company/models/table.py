from typing import Dict, Any

"""
  "table": {
    "name": "table_1"
  }
"""


class Table:

    def __init__(self, name: str) -> None:
        self.name: str = name

    def __repr__(self) -> str:
        repr: str = """
            Table(name={name})
        """.format(name=self.name)
        return repr

    @staticmethod
    def load(my_dict: Dict[str, Any]) -> 'Table':
        return Table(**my_dict)
