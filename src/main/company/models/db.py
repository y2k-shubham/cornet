from typing import Dict

"""
  "db": {
    "name": "db_1"
  }
"""


class Db:

    def __init__(self, name: str) -> None:
        self.name: str = name

    def __repr__(self) -> str:
        repr: str = """
            Db(name={name})
        """.format(name=self.name)
        return repr

    @staticmethod
    def load(my_dict: Dict[str, str]) -> 'Db':
        return Db(**my_dict)
