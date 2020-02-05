from typing import Dict, List, Tuple, Optional

from src.main.company.cmds.hive_beeline_cmd_prefix import HiveBeelineCmdPrefix


class HiveCreateTableCmd:
    # templates
    _cmd_template: str = ' \\\n  '.join([
        '{beeline_cmd_prefix} -e',
        '"CREATE TABLE \\`{db_name}\\`.\\`{table_name}\\` (',
        '  {columns})',
        '{partition}',
        '{file_format};"'
    ])
    _partition_template: str = "PARTITIONED BY (\\`{partition_column_name}\\` {partition_column_type})"
    _file_format_templates: Dict[str, str] = {
        'text': '',
        'orc': 'STORED AS ORC tblproperties (\\"orc.compress\\"=\\"SNAPPY\\")'
    }

    @staticmethod
    def _get_columns_string(columns: Dict[str, str]) -> str:
        columns_list: List[str] = [
            ' \\`{column_name}\\` {column_type}'.format(column_name=column_name, column_type=column_type) for
            column_name, column_type in
            columns.items()]
        columns_string: str = ', \\\n    '.join(columns_list)

        return columns_string

    @staticmethod
    def _get_partition_string(partition_column: Optional[Tuple[str, str]]) -> str:
        if partition_column:
            return HiveCreateTableCmd._partition_template.format(
                partition_column_name=partition_column[0],
                partition_column_type=partition_column[1]
            )
        else:
            return ''

    @staticmethod
    def get_cmd(
            db_table_name: Tuple[str, str],
            columns: Dict[str, str],
            partition_column: Optional[Tuple[str, str]] = None,
            file_format: str = "orc") -> str:
        db_name: str = db_table_name[0]
        table_name: str = db_table_name[1]
        beeline_cmd_prefix: str = HiveBeelineCmdPrefix.get_prefix()
        columns: str = HiveCreateTableCmd._get_columns_string(columns)
        partition: str = HiveCreateTableCmd._get_partition_string(partition_column)
        file_format: str = HiveCreateTableCmd._file_format_templates[file_format]

        cmd: str = HiveCreateTableCmd._cmd_template.format(
            beeline_cmd_prefix=beeline_cmd_prefix,
            db_name=db_name,
            table_name=table_name,
            columns=columns,
            partition=partition,
            file_format=file_format
        )
        return cmd
