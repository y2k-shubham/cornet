from typing import Tuple, Optional, Dict

from src.main.company.cmds.hive_beeline_cmd_prefix import HiveBeelineCmdPrefix


class HiveInsertSelectJoinCmd:
    _cmd_template: str = ' \\\n  '.join([
        '{beeline_cmd_prefix} -e',
        '"INSERT INTO TABLE',
        '  \\`{dst_db_name}\\`.\\`{dst_table_name}\\`',
        'PARTITION (\\`{partition_column_name}\\`)',
        'SELECT',
        '  {columns},',
        '  {partition_column_select_stmt} AS \\`{partition_column_name}\\`',
        'FROM',
        '  \\`{src_db_name}\\`.\\`{src_table_name}\\` AS \\`t1\\`',
        'INNER JOIN',
        '  \\`{with_db_name}\\`.\\`{with_table_name}\\` AS \\`t2\\`',
        'ON',
        '  \\`t2\\`.\\`{on_column}\\` = \\`t1\\`.\\`{using_column}\\`;"'
    ])

    @staticmethod
    def _get_columns_string(columns: Dict[str, Optional[str]]) -> str:
        def get_select_arg(column_name_select_stmt: Tuple[str, Optional[str]]) -> str:
            column_name: str = column_name_select_stmt[0]
            select_stmt: Optional[str] = column_name_select_stmt[1]
            if select_stmt:
                select_arg: str = "{select_stmt} AS \\`{column_name}\\`".format(
                    select_stmt=select_stmt,
                    column_name=column_name
                )
            else:
                select_arg: str = "\\`t1\\`.\\`{column_name}\\` AS \\`{column_name}\\`".format(column_name=column_name)
            return select_arg

        columns_string: str = ", \\\n    ".join(
            [get_select_arg(column_name_select_stmt) for column_name_select_stmt in
             columns.items()])

        return columns_string

    @staticmethod
    def get_cmd(
            src_db_table_name: Tuple[str, str],
            dst_db_table_name: Tuple[str, str],
            with_db_table_name: Tuple[str, str],
            columns: Dict[str, Optional[str]],
            on_using_columns: Tuple[str, str],
            partition_column_name_select_stmt: Tuple[str, str]) -> str:
        beeline_cmd_prefix: str = HiveBeelineCmdPrefix.get_prefix()
        src_db_name, src_table_name = src_db_table_name
        dst_db_name, dst_table_name = dst_db_table_name
        with_db_name, with_table_name = with_db_table_name
        columns: str = HiveInsertSelectJoinCmd._get_columns_string(columns)
        on_column, using_column = on_using_columns
        partition_column_name, partition_column_select_stmt = partition_column_name_select_stmt

        cmd: str = HiveInsertSelectJoinCmd._cmd_template.format(
            beeline_cmd_prefix=beeline_cmd_prefix,
            dst_db_name=dst_db_name,
            dst_table_name=dst_table_name,
            partition_column_name=partition_column_name,
            columns=columns,
            partition_column_select_stmt=partition_column_select_stmt,
            src_db_name=src_db_name,
            src_table_name=src_table_name,
            with_db_name=with_db_name,
            with_table_name=with_table_name,
            on_column=on_column,
            using_column=using_column
        )
        return cmd
