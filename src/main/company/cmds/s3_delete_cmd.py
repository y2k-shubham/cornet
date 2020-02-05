from typing import Tuple

from src.main.company.config.config import Config


class S3DeleteCmd:
    # templates
    _cmd_template: str = ' \\\n  '.join([
        'aws',
        's3 rm',
        's3://{bucket}/{db_name}.db/{table_name}/',
        '--recursive'
    ])
    _s3_bucket: str = Config.get_config("s3_bucket")

    @staticmethod
    def get_cmd(db_table_name: Tuple[str, str]) -> str:
        # parameters
        db_name, table_name = db_table_name

        # cmd
        cmd: str = S3DeleteCmd._cmd_template.format(
            bucket=S3DeleteCmd._s3_bucket,
            db_name=db_name,
            table_name=table_name
        )

        return cmd
