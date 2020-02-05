from typing import Tuple, Dict, Optional, List

from src.main.company.cmds.hadoop_distcp_cmd import HadoopDistcpCmd
from src.main.company.cmds.hadoop_rm_cmd import HadoopRmCmd
from src.main.company.cmds.hive_create_table_cmd import HiveCreateTableCmd
from src.main.company.cmds.hive_drop_cmd import HiveDropCmd
from src.main.company.cmds.hive_insert_select_cmd import HiveInsertSelectCmd
from src.main.company.cmds.hive_insert_select_join_cmd import HiveInsertSelectJoinCmd
from src.main.company.cmds.hive_msck_repair_cmd import HiveMsckRepairCmd
from src.main.company.cmds.s3_delete_cmd import S3DeleteCmd
from src.main.company.cmds.sqoop_cmd import SqoopCmd
from src.main.company.models.hive_join_step import HiveJoinStep
from src.main.company.models.hive_orc_step import HiveOrcStep
from src.main.company.models.sqoop_step import SqoopStep
from src.main.company.models.task_config import TaskConfig
from src.main.company.utils.dict_utils import merge_dict, append_to_indexed_dict
from src.main.company.utils.schema_utils import get_hive_schema, extract_select_stmt_for_column, get_mysql_schema
from src.main.company.utils.string_utils import get_tmp_table_names


class CmdsCreator:

    @staticmethod
    def get_hive_drop_cmds_dict(task_config: TaskConfig) -> Dict[str, str]:
        db_name: str = task_config.db.name
        table_name: str = task_config.table.name
        tmp_table_names: Tuple[str, str] = get_tmp_table_names(table_name)

        cmds_dict: Dict[str, str] = {}
        cmds_dict['hive_drop_table_cmd']: str = HiveDropCmd.get_cmd((db_name, table_name))
        if task_config.is_orc():
            cmds_dict['hive_drop_tmp_table_1_cmd']: str = HiveDropCmd.get_cmd((db_name, tmp_table_names[0]))
            if task_config.is_joined():
                cmds_dict['hive_drop_tmp_table_2_cmd']: str = HiveDropCmd.get_cmd((db_name, tmp_table_names[1]))

        return cmds_dict

    @staticmethod
    def get_hadoop_rm_cmds_dict(task_config: TaskConfig) -> Dict[str, str]:
        db_name: str = task_config.db.name
        table_name: str = task_config.table.name
        tmp_table_names: Tuple[str, str] = get_tmp_table_names(table_name)

        cmds_dict: Dict[str, str] = {}
        cmds_dict['hadoop_rm_table_cmd_1']: str = HadoopRmCmd.get_cmd((db_name, table_name))
        cmds_dict['hadoop_rm_table_cmd_2']: str = HadoopRmCmd.get_cmd((db_name, table_name), use_hive_dir=True)
        if task_config.is_orc():
            cmds_dict['hadoop_rm_tmp_table_1_cmd']: str = HadoopRmCmd.get_cmd((db_name, tmp_table_names[0]))
            if task_config.is_joined():
                cmds_dict['hadoop_rm_tmp_table_2_cmd']: str = HadoopRmCmd.get_cmd((db_name, tmp_table_names[1]))

        return cmds_dict

    @staticmethod
    def get_sqoop_cmd_dict(task_config: TaskConfig, mysql_schema: Dict[str, str]) -> Dict[str, str]:
        db_name: str = task_config.db.name
        table_name: str = task_config.table.name
        dst_table_name: str = get_tmp_table_names(table_name)[0] if task_config.is_orc() else table_name
        sqoop_step: SqoopStep = task_config.sqoop_step

        # params
        src_db_table_name: Tuple[str, str] = (db_name, table_name)
        dst_db_table_name: Tuple[str, str] = (db_name, dst_table_name)
        columns: Optional[List[str]] = mysql_schema.keys() if sqoop_step.select_columns else None

        cmd: str = SqoopCmd.get_cmd(
            src_db_table_name=src_db_table_name,
            dst_db_table_name=dst_db_table_name,
            columns=columns,
            map_columns_java=sqoop_step.map_columns_java,
            map_columns_hive=sqoop_step.map_columns_hive
        )
        cmds_dict: Dict[str, str] = {'sqoop_cmd': cmd}

        return cmds_dict

    @staticmethod
    def get_hive_create_tmp_table_2_cmd_dict(task_config: TaskConfig, hive_schema: Dict[str, str]) -> Dict[str, str]:
        if task_config.is_joined():
            db_name: str = task_config.db.name
            table_name: str = task_config.table.name
            hive_orc_step: HiveOrcStep = task_config.hive_orc_step

            # params
            db_tmp_table_2_name: Tuple[str, str] = (db_name, get_tmp_table_names(table_name)[1])

            # cmd
            cmd: str = HiveCreateTableCmd.get_cmd(
                db_table_name=db_tmp_table_2_name,
                columns=hive_schema,
                partition_column=None
            )
            cmd_dict: Dict[str, str] = {'hive_create_tmp_table_2_cmd': cmd}

            return cmd_dict
        else:
            return {}

    @staticmethod
    def get_hive_create_table_cmd_dict(task_config: TaskConfig, hive_schema: Dict[str, str]) -> Dict[str, str]:
        if task_config.is_orc():
            db_table_name: Tuple[str, str] = (task_config.db.name, task_config.table.name)
            if task_config.is_joined():
                # params
                partition_column: Tuple[str, str] = (
                    task_config.hive_join_step.partition['name'],
                    task_config.hive_join_step.partition['type'])

                # cmd
                cmd: str = HiveCreateTableCmd.get_cmd(
                    db_table_name=db_table_name,
                    columns=hive_schema,
                    partition_column=partition_column
                )
            else:
                hive_orc_step: HiveOrcStep = task_config.hive_orc_step

                # params
                partition_column: Tuple[str, str] = (
                    hive_orc_step.partition['name'],
                    hive_orc_step.partition['type']) if hive_orc_step.partition else None

                # cmd
                cmd: str = HiveCreateTableCmd.get_cmd(
                    db_table_name=db_table_name,
                    columns=hive_schema,
                    partition_column=partition_column
                )

            cmd_dict: Dict[str, str] = {'hive_create_table_cmd': cmd}
            return cmd_dict
        else:
            return {}

    @staticmethod
    def get_hive_insert_select_cmd_dict(
            task_config: TaskConfig,
            mysql_schema: Dict[str, str],
            hive_schema: Dict[str, str]) -> Dict[str, str]:
        """
        - Hive-beeline cmd that selects data from one Hive table
          and inserts it into another Hive table
        - Used for converting table data from text format to ORC format
        - Only for tables in ORC format
        - May involve partitioning
        - Source table is tmp_table_1
        - If table has a HiveJoinStep, then destination table is tmp_table_2,
          otherwise it is final table
        :param task_config:  Model representing table sync task
        :type task_config:   TaskConfig
        :param mysql_schema: Schema of table as in MySQL
        :type mysql_schema:  Dict[str, str] (column_name -> column_type)
        :param hive_schema:  Schema of table as in Hive
                             (excluding add_columns & partition_column, if any)
        :type hive_schema:   Dict[str, str] (column_name -> column_type)
        :return:             Dict containing Hive INSERT..SELECT cmd
                              - Key of dict is name of cmd: 'hive_insert_select_cmd'
                              - Value of dict is the actual string representing cmd
        :type:               Dict[str, str]
        """
        if task_config.is_orc():
            db_name: str = task_config.db.name
            table_name: str = task_config.table.name
            dst_table_name: str = get_tmp_table_names(table_name)[1] if task_config.is_joined() else table_name
            hive_orc_step: HiveOrcStep = task_config.hive_orc_step

            # params
            src_db_table_name: Tuple[str, str] = (db_name, get_tmp_table_names(table_name)[0])
            dst_db_table_name: Tuple[str, str] = (db_name, dst_table_name)
            if hive_orc_step.select_columns or hive_orc_step.add_columns or hive_orc_step.partition:
                """
                hive_schema includes add_columns, but it is a mapping
                between column_name and column_type. However we want mapping
                between column_name and select_stmt. Thats all that
                is done in following statements
                """
                columns_with_types: Dict[str, str] = hive_schema
                add_columns: Optional[List[Dict[str, str]]] = hive_orc_step.add_columns
                partition: Optional[Dict[str, str]] = hive_orc_step.partition

                add_columns_with_partition: List[Dict[str, str]] = []
                # include new columns to be added
                if add_columns:
                    add_columns_with_partition.extend(add_columns)
                # include partition column
                if partition:
                    add_columns_with_partition.append(partition)
                    partition_column_name: str = partition['name']
                else:
                    partition_column_name: Optional[str] = None

                existing_columns_with_select_stmt: Dict[str, Optional[str]] = {
                    column_name: extract_select_stmt_for_column(column_name, add_columns_with_partition)
                    for column_name in columns_with_types.keys()
                }
                add_columns_with_partition_with_select_stmt: Dict[str, str] = {
                    column_dict['name']: column_dict['select_stmt'] for column_dict in add_columns_with_partition
                }
                all_columns_with_select_stmt: Dict[str, Optional[str]] = merge_dict(
                    existing_columns_with_select_stmt, add_columns_with_partition_with_select_stmt)

                columns: Dict[str, Optional[str]] = all_columns_with_select_stmt

                if len(mysql_schema) == len(columns) and all(value is None for value in columns.values()):
                    columns: Optional[Dict[str, Optional[str]]] = None
            else:
                columns: Optional[Dict[str, Optional[str]]] = None
                partition_column_name: Optional[str] = None

            # cmd
            cmd: str = HiveInsertSelectCmd.get_cmd(
                src_db_table_name=src_db_table_name,
                dst_db_table_name=dst_db_table_name,
                columns=columns,
                partition_column_name=partition_column_name
            )
            cmd_dict: Dict[str, str] = {'hive_insert_select_cmd': cmd}

            return cmd_dict
        else:
            return {}

    @staticmethod
    def get_hive_insert_select_join_cmd_dict(task_config: TaskConfig, hive_schema: Dict[str, str]) -> Dict[
        str, str]:
        """
        - Hive beeline cmd that selects data from one Hive table
          and inserts it into another Hive table while
          partitioning it on a column obtained by joining it with
          an existing Hive table
        - Used for partitioning large tables that don't have
          a date-time type column
        - Only for large ORC tables lacking a datetime column
        - Involves partitioning
        - Source table is tmp_table_2
        - Destination table is final table
        :param task_config: Model representing table-sync task
        :type task_config:  TaskConfig
        :param hive_schema: Shema of table as in Hive
                            (excluding add_columns and partition_column, if any)
        :type hive_schema:  Dict[str, str] (column_name -> column_type)
        :return:            Dict containing Hive INSERT..SELECT..JOIN cmd
                             - Key of dict is name of cmd: 'hive_insert_select_join_cmd'
                             - Value of dict is actual string representing cmd
        :type:              Dict[str, str]
        """
        if task_config.is_joined():
            db_name: str = task_config.db.name
            table_name: str = task_config.table.name
            hive_join_step: HiveJoinStep = task_config.hive_join_step

            # params
            src_db_table_name: Tuple[str, str] = (db_name, get_tmp_table_names(table_name)[1])
            dst_db_table_name: Tuple[str, str] = (db_name, table_name)
            with_db_table_name: Tuple[str, str] = (db_name, hive_join_step.with_table)
            columns: Dict[str, Optional[str]] = {column_name: None for column_name in hive_schema.keys()}
            on_using_columns: Tuple[str, str] = (hive_join_step.on_column, hive_join_step.using_column)
            partition_column_name_select_stmt: Tuple[str, str] = (
                hive_join_step.partition['name'], hive_join_step.partition['select_stmt'])

            # cmd
            cmd: str = HiveInsertSelectJoinCmd.get_cmd(
                src_db_table_name=src_db_table_name,
                dst_db_table_name=dst_db_table_name,
                with_db_table_name=with_db_table_name,
                columns=columns,
                on_using_columns=on_using_columns,
                partition_column_name_select_stmt=partition_column_name_select_stmt
            )
            cmds_dict: Dict[str, str] = {'hive_insert_select_join_stmt': cmd}

            return cmds_dict
        else:
            return {}

    @staticmethod
    def get_s3_delete_cmd_dict(task_config: TaskConfig) -> Dict[str, str]:
        # params
        db_table_name: Tuple[str, str] = (task_config.db.name, task_config.table.name)

        # cmd
        cmd: str = S3DeleteCmd.get_cmd(db_table_name=db_table_name)
        cmd_dict: Dict[str, str] = {'s3_delete_cmd': cmd}

        return cmd_dict

    @staticmethod
    def get_hadoop_distcp_cmd_dict(task_config: TaskConfig) -> Dict[str, str]:
        # params
        db_table_name: Tuple[str, str] = (task_config.db.name, task_config.table.name)

        # cmd
        cmd: str = HadoopDistcpCmd.get_cmd(db_table_name=db_table_name)
        cmd_dict: Dict[str, str] = {'hadoop_distcp_cmd': cmd}

        return cmd_dict

    @staticmethod
    def get_hive_msck_repair_cmd_dict(task_config: TaskConfig) -> Dict[str, str]:
        if task_config.is_partitioned():
            # params
            db_table_name: Tuple[str, str] = (task_config.db.name, task_config.table.name)

            # cmd
            cmd: str = HiveMsckRepairCmd.get_cmd(db_table_name=db_table_name)
            cmd_dict: Dict[str, str] = {'hive_msck_repair_cmd': cmd}

            return cmd_dict
        else:
            return {}

    @staticmethod
    def get_cmds_dict(task_config: TaskConfig) -> Dict[int, Dict[str, str]]:
        # params
        db_name: str = task_config.db.name
        table_name: str = task_config.table.name
        db_table_name: Tuple[str, str] = (db_name, table_name)
        sqoop_step: SqoopStep = task_config.sqoop_step

        # init stage
        cmds_dict: Dict[int, Dict[str, str]] = {}
        cmds_dict: Dict[int, Dict[str, str]] = append_to_indexed_dict(cmds_dict, merge_dict(
            CmdsCreator.get_hive_drop_cmds_dict(task_config), CmdsCreator.get_hadoop_rm_cmds_dict(task_config)))

        # sqoop stage
        mysql_schema: Dict[str, str] = get_mysql_schema(
            db_table_name=db_table_name,
            select_columns=sqoop_step.select_columns,
            drop_selected=sqoop_step.drop_selected
        )
        cmds_dict: Dict[int, Dict[str, str]] = append_to_indexed_dict(
            cmds_dict,
            CmdsCreator.get_sqoop_cmd_dict(
                task_config=task_config,
                mysql_schema=mysql_schema
            )
        )

        if task_config.is_orc():
            # ORC conversion stage
            hive_orc_step: HiveOrcStep = task_config.hive_orc_step
            hive_schema: Dict[str, str] = get_hive_schema(
                existing_schema=mysql_schema,
                select_columns=hive_orc_step.select_columns,
                drop_selected=hive_orc_step.drop_selected,
                add_columns=hive_orc_step.add_columns
            )

            if task_config.is_joined():
                my_dict_1: Dict[str, str] = CmdsCreator.get_hive_create_tmp_table_2_cmd_dict(task_config,
                                                                                             hive_schema)
            else:
                my_dict_1: Dict[str, str] = CmdsCreator.get_hive_create_table_cmd_dict(task_config, hive_schema)
            my_dict_2: Dict[str, str] = CmdsCreator.get_hive_insert_select_cmd_dict(task_config, mysql_schema,
                                                                                    hive_schema)
            cmds_dict: Dict[int, Dict[str, str]] = append_to_indexed_dict(cmds_dict, merge_dict(my_dict_1, my_dict_2))

            # join stage
            if task_config.is_joined():
                my_dict_3: Dict[str, str] = CmdsCreator.get_hive_create_table_cmd_dict(task_config, hive_schema)
                my_dict_4: Dict[str, str] = CmdsCreator.get_hive_insert_select_join_cmd_dict(task_config,
                                                                                             hive_schema)
                cmds_dict: Dict[int, Dict[str, str]] = append_to_indexed_dict(cmds_dict,
                                                                              merge_dict(my_dict_3, my_dict_4))

        # s3 copy stage
        my_dict_5: Dict[str, str] = CmdsCreator.get_s3_delete_cmd_dict(task_config)
        my_dict_6: Dict[str, str] = CmdsCreator.get_hadoop_distcp_cmd_dict(task_config)
        cmds_dict: Dict[int, Dict[str, str]] = append_to_indexed_dict(cmds_dict, merge_dict(my_dict_5, my_dict_6))

        # msck repair stage
        if task_config.is_partitioned():
            cmds_dict: Dict[int, Dict[str, str]] = append_to_indexed_dict(
                cmds_dict,
                CmdsCreator.get_hive_msck_repair_cmd_dict(task_config=task_config)
            )

        return cmds_dict
