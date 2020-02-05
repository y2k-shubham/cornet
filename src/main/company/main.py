from typing import List, Callable

import click

from src.main.company.cmds.sqoop_cmd import SqoopCmd
from src.main.company.connectors import Table
from src.main.company.connectors import get_connector
from src.main.company.models.column import Column
from src.main.company.models.task_config import TaskConfig
from src.main.company.utils.string_utils import match_any


def print_sqoop_cmds(task: TaskConfig) -> None:
    with get_connector(task.source) as conn:
        to_import: List[Table] = get_tables_to_import(conn, task)
        for table in sorted(to_import, key=lambda tbl: tbl.name):
            columns: List[Column] = conn.get_columns(table)
            cmd: SqoopCmd = SqoopCmd(task, table, columns)
            print(cmd.as_string())


def get_tables_to_import(conn, task: TaskConfig) -> List[Table]:
    all_tables: List[Table] = conn.get_tables()
    is_imported: Callable[[Table], bool] = lambda table: not task.import_tables or \
                                                         match_any(task.import_tables, table.name)
    is_skipped: Callable[[Table], bool] = lambda table: match_any(task.skip_tables, table.name)
    return [t for t in all_tables if is_imported(t) and not is_skipped(t)]


def print_schema(task: TaskConfig) -> None:
    with get_connector(task.source) as conn:
        to_import: List[Table] = get_tables_to_import(conn, task)
        for table in to_import:
            print('\n=== {0}.{1} ==='.format(task.source['db'], table.name))
            columns: List[Column] = conn.get_columns(table)
            for column in columns:
                print('{0}: {1}'.format(column.name, column.sql_type))


@click.command()
@click.argument('config_filename', type=click.Path(exists=True))
@click.option('--print-schema-only', is_flag=True)
def cli(config_filename: str, print_schema_only: bool):
    for task in TaskConfig.load(config_filename):
        if print_schema_only:
            print_schema(task)
        else:
            print_sqoop_cmds(task)
