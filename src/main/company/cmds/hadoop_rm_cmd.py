from typing import Tuple

from src.main.company.config.config import Config
from src.main.company.utils.string_utils import split_on_first_occurrence


class HadoopRmCmd:
    # templates
    _cmd_template: str = ' \\\n  '.join([
        'hdfs dfs',
        '-rm -R',
        '{hdfs_path}/{db_part}{table_name}'
    ])
    _db_part_template: str = '{db_name}.db/'

    # configs
    _warehouse_dir: str = Config.get_config("warehouse_dir")
    # reverse Python string: https://stackoverflow.com/questions/931092/
    _hive_dir: str = split_on_first_occurrence(_warehouse_dir[::-1], '/')[1][::-1]

    @staticmethod
    def get_cmd(db_table_name: Tuple[str, str], use_hive_dir: bool = False) -> str:
        # parameters
        db_name, table_name = db_table_name
        if use_hive_dir:
            hdfs_path: str = HadoopRmCmd._hive_dir
            db_part: str = ''
        else:
            hdfs_path: str = HadoopRmCmd._warehouse_dir
            db_part: str = HadoopRmCmd._db_part_template.format(db_name=db_name)

        # build command
        cmd: str = HadoopRmCmd._cmd_template.format(
            hdfs_path=hdfs_path,
            db_part=db_part,
            table_name=table_name)

        return cmd
