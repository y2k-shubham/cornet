from typing import Tuple

from src.main.company.config.config import Config


class HadoopDistcpCmd:
    # templates
    _cmd_template: str = ' \\\n  '.join([
        'sudo',
        '-u hive',
        'hadoop distcp',
        '-m {m}',
        '{warehouse_dir}/{db_name}.db/{table_name}/',
        's3a://{bucket}/{db_name}.db/{table_name}/'
    ])
    _distcp_m: int = Config.get_config("distcp_m")
    _s3_bucket: str = Config.get_config("s3_bucket")
    _warehouse_dir: str = Config.get_config("warehouse_dir")

    @staticmethod
    def get_cmd(db_table_name: Tuple[str, str]) -> str:
        # parameters
        db_name, table_name = db_table_name

        # cmd
        cmd: str = HadoopDistcpCmd._cmd_template.format(
            m=HadoopDistcpCmd._distcp_m,
            bucket=HadoopDistcpCmd._s3_bucket,
            warehouse_dir=HadoopDistcpCmd._warehouse_dir,
            db_name=db_name,
            table_name=table_name
        )

        return cmd
