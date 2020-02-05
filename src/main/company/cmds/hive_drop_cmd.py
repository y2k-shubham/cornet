from typing import Tuple

from src.main.company.cmds.hive_beeline_cmd_prefix import HiveBeelineCmdPrefix


class HiveDropCmd:
    # templates
    _cmd_template: str = ' \\\n  '.join([
        '{beeline_cmd_prefix} -e',
        '"DROP TABLE IF EXISTS \\`{db_name}\\`.\\`{table_name}\\`;"'
    ])

    @staticmethod
    def get_cmd(db_table_name: Tuple[str, str]) -> str:
        beeline_cmd_prefix: str = HiveBeelineCmdPrefix.get_prefix()
        db_name: str = db_table_name[0]
        table_name: str = db_table_name[1]

        # build command
        cmd: str = HiveDropCmd._cmd_template.format(
            beeline_cmd_prefix=beeline_cmd_prefix,
            db_name=db_name,
            table_name=table_name
        )

        return cmd
