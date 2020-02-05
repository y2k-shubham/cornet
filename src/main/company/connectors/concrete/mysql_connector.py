from typing import Dict, List, Any

import MySQLdb
from MySQLdb.connections import Connection

from src.main.company.connectors import Table
from src.main.company.connectors.base_connector import BaseConnector
from src.main.company.models.column import Column


class MySqlConnector(BaseConnector):
    jdbc_url_prefix: str = 'jdbc:mysql'

    def _get_db_conn(self) -> Connection:
        source: Dict[str, Any] = self.source
        return MySQLdb.connect(
            host=source['host'],
            port=source['port'],
            user=source['user'],
            passwd=self._get_password(),
            db=source['db'])

    def get_tables(self) -> List[Table]:
        sql_query: str = "show full tables"
        query_result: List[Any] = self.query(sql_query)
        return list(map(Table._make, query_result))

    def get_columns(self, table: Table) -> List[Column]:
        sql_query: str = """
            SELECT
                `column_name` AS `name`,
                UPPER(`data_type`) AS `sql_type`
            FROM
                `information_schema`.`columns`
            WHERE
                `table_schema` = '{db_name}'
                AND `table_name` = '{table_name}'
        """
        query_result: List[Any] = self.query(sql_query.format(db_name=self.source['db'], table_name=table.name))
        return list(map(lambda tup: Column(name=tup[0], sql_type=tup[1]), query_result))
