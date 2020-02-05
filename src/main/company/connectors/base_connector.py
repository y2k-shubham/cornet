from contextlib import closing
from typing import Dict, Any, List


class BaseConnector:

    def __init__(self, source: Dict[str, Any]) -> None:
        self.source: Dict[str, Any] = source

    def __enter__(self) -> object:
        self.db_conn = self._get_db_conn()
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.db_conn.close()

    def _get_db_conn(self):
        raise NotImplementedError()

    def query(self, sql_query: str) -> List[Any]:
        with closing(self.db_conn.cursor()) as cursor:
            cursor.execute(sql_query)
            return cursor.fetchall() or []

    def _get_password(self) -> str:
        if 'password' in self.source:
            return self.source['password']
        else:
            with open(self.source['password_file'], 'r') as f:
                return f.readline().strip()
