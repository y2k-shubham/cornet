from src.main.company.config.config import Config


class HiveBeelineCmdPrefix:
    # templates
    _prefix_template: str = ' \\\n  '.join([
        'sudo',
        '-u hive',
        'beeline',
        '--showheader=false',
        '--outputformat=tsv2',
        '-u jdbc:hive2://{host}:10000/',
        '-n hive',
        ''
    ])
    _confs: str = ' \\\n  '.join([
        '--hiveconf hive.exec.max.dynamic.partitions=100000',
        '--hiveconf hive.exec.max.dynamic.partitions.pernode=100000',
        '--hiveconf hive.execution.engine=tez',
        ''
    ])

    @staticmethod
    def get_prefix(use_prod: bool = False, add_confs: bool = True) -> str:
        # host
        host_to_use: str = Config.get_config("hive_metastore/production") if use_prod else Config.get_config(
            "hive_metastore/local")

        # build command
        prefix_cmd_without_confs: str = HiveBeelineCmdPrefix._prefix_template.format(host=host_to_use)
        prefix_cmd_with_confs: str = (
                prefix_cmd_without_confs + HiveBeelineCmdPrefix._confs) if add_confs else prefix_cmd_without_confs

        return prefix_cmd_with_confs
