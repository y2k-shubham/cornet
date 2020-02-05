from datetime import datetime, timedelta
from typing import Optional, List

from src.main.company.models.db import Db
from src.main.company.models.table import Table
from src.main.company.utils.datetime_utils import get_duration_str


class DbMetric:

    def __init__(
            self,
            db: Db,
            start_time: datetime,
            end_time: Optional[datetime] = None) -> None:
        self.db: Db = db
        self.start_time: datetime = start_time
        self.end_time: Optional[datetime] = end_time
        self.failed_tables: List[Table] = []

    def __repr__(self) -> str:
        failed_table_names: List[str] = [table.name for table in self.failed_tables]
        if self.end_time:
            duration: timedelta = self.end_time - self.start_time
            duration_str: str = get_duration_str(duration)
        else:
            duration_str: str = '-'

        repr: str = """
            DbMetric(
                db={db_name},
                
                start_time={start_time},
                end_time={end_time},
                duration={duration},
                
                failed_tables={failed_table_names}
            )
        """.format(
            db_name=self.db.name,

            start_time=self.start_time,
            end_time=self.end_time,
            duration=duration_str,

            failed_table_names=failed_table_names
        )
        return repr

    def add_failed_table(self, table: Table) -> None:
        self.failed_tables.append(table)

    def get_failed_table_names(self) -> List[str]:
        failed_table_names: List[str] = [table.name for table in self.failed_tables]
        return failed_table_names

    def get_failed_tables_count(self) -> int:
        return len(self.failed_tables)
