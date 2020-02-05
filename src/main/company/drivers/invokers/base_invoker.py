from multiprocessing.dummy import Pool
from multiprocessing.pool import ThreadPool
from typing import Dict, List, Optional, Callable, Tuple

from src.main.company.drivers.task_config_reader import TaskConfigReader
from src.main.company.metrics.db_metric import DbMetric
from src.main.company.metrics.table_metric import TableMetric
from src.main.company.models.task_config import TaskConfig
from src.main.company.utils.metric_utils import group_table_metrics_by_db, create_db_metric, filter_table_metrics_by_db
from src.main.company.utils.print_utils import separators, print_with_new_lines
from src.main.company.utils.process_utils import process_task_config


class BaseInvoker:

    def __init__(
            self,
            file_paths: List[str],
            pool_size: int) -> None:
        self.file_paths: List[str] = file_paths
        self.pool_size: int = pool_size
        self.pool: ThreadPool = Pool(pool_size)
        self.task_config_groups: Optional[Dict[str, List[TaskConfig]]] = None
        self.table_metrics: List[TableMetric] = []
        self.db_metrics: Dict[str, DbMetric] = {}

    def read_task_configs(self) -> None:
        self.task_config_groups: Dict[str, List[TaskConfig]] = TaskConfigReader.get_task_configs(self.file_paths)

    def process_task_configs(
            self,
            show: bool = True,
            execute: bool = False) -> None:
        if self.task_config_groups is None:
            self.read_task_configs()

        def task_configs_processor(show: bool, execute: bool) -> Callable[[List[TaskConfig]], None]:
            def actual_processor(task_configs: List[TaskConfig]) -> None:
                for task_config in task_configs:
                    table_metric: TableMetric = TableMetric(task_config)
                    failure_tuple: Optional[Tuple[str, Exception]] = process_task_config(task_config, show, execute)
                    if failure_tuple:
                        table_metric.set_completed(failed=True)
                        table_metric.failure_step, table_metric.exception = failure_tuple
                    else:
                        table_metric.set_completed()
                    self.table_metrics.append(table_metric)

            return actual_processor

        self.pool.map(task_configs_processor(show, execute), self.task_config_groups.values())
        self.calc_db_metric()

    def calc_db_metric(self) -> None:
        grouped_table_metrics: Dict[str, List[TableMetric]] = group_table_metrics_by_db(self.table_metrics)
        self.db_metrics: Dict[str, DbMetric] = {db_name: create_db_metric(table_metrics) for db_name, table_metrics in
                                                grouped_table_metrics.items()}

    def show_table_metrics(self) -> None:
        for db_name in self.db_metrics.keys():
            print_with_new_lines(separators['-'])
            print_with_new_lines(db_name, caption='Db', new_line_before=False, new_line_after=False)
            print_with_new_lines(separators['-'])

            filtered_table_metrics: List[TableMetric] = filter_table_metrics_by_db(self.table_metrics, db_name)
            for table_metric in filtered_table_metrics:
                print_with_new_lines(separators['.'])
                print_with_new_lines(table_metric, new_line_before=False, new_line_after=False)
                print_with_new_lines(separators['.'])

    def show_db_metrics(self) -> None:
        for db_name, db_metric in self.db_metrics.items():
            print_with_new_lines(separators['-'])
            print_with_new_lines(db_metric, new_line_before=False, new_line_after=False)
            print_with_new_lines(separators['-'])

    def show_failed_tables(self) -> None:
        for db_name, failure_tuples in self.failed_tables.items():
            if failure_tuples:
                print_with_new_lines(separators['-'])
                print_with_new_lines(db_name, caption='Db', new_line_before=False, new_line_after=False)
                print_with_new_lines(separators['-'])

                for failure_tuple in failure_tuples:
                    print_with_new_lines(separators[':'], new_line_after=False)

                    print_with_new_lines(failure_tuple[0], caption='Table')
                    print_with_new_lines(separators['.'], new_line_before=False, new_line_after=False)
                    print_with_new_lines(failure_tuple[1], caption='Failed-Cmd')
                    print_with_new_lines(separators['.'], new_line_before=False, new_line_after=False)
                    print_with_new_lines(failure_tuple[2].args[0], caption='StackTrace')

                    print_with_new_lines(separators[':'], new_line_before=False)
