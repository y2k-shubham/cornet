from datetime import datetime, timezone, timedelta
from typing import Optional

from src.main.company.models.db import Db
from src.main.company.models.table import Table
from src.main.company.models.task_config import TaskConfig
from src.main.company.utils.datetime_utils import get_duration_str


class TableMetric:

    def __init__(self, task_config: TaskConfig) -> None:
        self.db: Db = task_config.db
        self.table: Table = task_config.table
        self.start_time: datetime = datetime.now(timezone.utc)
        self.end_time: Optional[datetime] = None
        self.failed: bool = False
        self.failure_step: Optional[str] = None
        self.exception: Optional[Exception] = None

    def __repr__(self) -> str:
        exception_str: str = self.exception.args[0] if self.exception else ""
        if self.end_time:
            duration: timedelta = self.end_time - self.start_time
            duration_str: str = get_duration_str(duration)
        else:
            duration_str: str = '-'

        repr: str = """
            TableMetric(
                db={db_name},
                table={table_name},
                
                start_time={start_time},
                end_time={end_time},
                duration={duration},
                
                failed={failed},
                failure_step={failure_step},
                exception={exception}
            )
        """.format(
            db_name=self.db.name,
            table_name=self.table.name,

            start_time=self.start_time,
            end_time=self.end_time,
            duration=duration_str,

            failed=self.failed,
            failure_step=self.failure_step,
            exception=exception_str
        )
        return repr

    def set_completed(self, failed: bool = False) -> None:
        self.end_time: datetime = datetime.now(timezone.utc)
        self.failed: bool = failed

    def set_failed(self, failure_step: str, exception: Optional[Exception] = None) -> None:
        self.set_completed(True)
        self.failure_step: str = failure_step
        self.exception: Optional[Exception] = exception

    def did_fail(self) -> bool:
        return self.failed or (self.end_time is None)
