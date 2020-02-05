from typing import Tuple, Optional, Dict

from src.main.company.cmds.hive_beeline_cmd_prefix import HiveBeelineCmdPrefix


class HiveInsertSelectCmd:
    _cmd_template: str = ' \\\n  '.join([
        '{beeline_cmd_prefix} -e',
        '"INSERT INTO TABLE',
        '  \\`{dst_db_name}\\`.\\`{dst_table_name}\\`',
        '{partition}',
        'SELECT',
        '  {columns}',
        'FROM',
        '  \\`{src_db_name}\\`.\\`{src_table_name}\\`;"'
    ])
    _partition_template: str = "PARTITION (\\`{partition_column_name}\\`)"

    @staticmethod
    def _get_partition_string(partition_column_name: Optional[str]) -> str:
        if partition_column_name:
            return HiveInsertSelectCmd._partition_template.format(partition_column_name=partition_column_name)
        else:
            return ''

    @staticmethod
    def _get_columns_string(columns: Optional[Dict[str, Optional[str]]]) -> str:
        def get_select_arg(column_name_select_stmt: Tuple[str, Optional[str]]) -> str:
            column_name: str = column_name_select_stmt[0]
            select_stmt: Optional[str] = column_name_select_stmt[1]
            if select_stmt:
                select_arg: str = "{select_stmt} AS \\`{column_name}\\`".format(
                    select_stmt=select_stmt,
                    column_name=column_name
                )
            else:
                select_arg: str = "\\`{column_name}\\`".format(column_name=column_name)
            return select_arg

        columns_string: str = ", \\\n    ".join(
            [get_select_arg(column_name_select_stmt) for column_name_select_stmt in
             columns.items()]) if columns else '*'

        return columns_string

    @staticmethod
    def get_cmd(
            src_db_table_name: Tuple[str, str],
            dst_db_table_name: Tuple[str, str] = None,
            columns: Optional[Dict[str, Optional[str]]] = None,
            partition_column_name: Optional[str] = None) -> str:
        beeline_cmd_prefix: str = HiveBeelineCmdPrefix.get_prefix()
        src_db_name, src_table_name = src_db_table_name
        dst_db_name, dst_table_name = dst_db_table_name
        columns: str = HiveInsertSelectCmd._get_columns_string(columns)
        partition: str = HiveInsertSelectCmd._get_partition_string(partition_column_name)

        cmd: str = HiveInsertSelectCmd._cmd_template.format(
            beeline_cmd_prefix=beeline_cmd_prefix,
            dst_db_name=dst_db_name,
            dst_table_name=dst_table_name,
            partition=partition,
            columns=columns,
            src_db_name=src_db_name,
            src_table_name=src_table_name
        )
        return cmd
