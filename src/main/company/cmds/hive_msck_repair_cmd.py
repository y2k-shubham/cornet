from typing import Tuple

from src.main.company.cmds.hive_beeline_cmd_prefix import HiveBeelineCmdPrefix


class HiveMsckRepairCmd:
    # templates
    _cmd_template: str = ' \\\n  '.join([
        '{beeline_cmd_prefix} -e',
        '"MSCK REPAIR TABLE \\`{db_name}\\`.\\`{table_name}\\`;"'
    ])

    @staticmethod
    def get_cmd(db_table_name: Tuple[str, str]) -> str:
        # parameters
        beeline_cmd_prefix: str = HiveBeelineCmdPrefix.get_prefix(use_prod=True, add_confs=False)
        db_name, table_name = db_table_name

        # build command
        cmd: str = HiveMsckRepairCmd._cmd_template.format(
            beeline_cmd_prefix=beeline_cmd_prefix,
            db_name=db_name,
            table_name=table_name
        )

        return cmd
