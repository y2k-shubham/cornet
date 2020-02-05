from typing import Dict, Any, List, Union, Optional, Tuple

from src.main.company.config.config import Config
from src.main.company.config.setting import Setting
from src.main.company.utils.dict_utils import merge_dict


class SqoopCmd:
    _sqoop_cmd_prefix: str = 'sudo -u hive sqoop import '
    _jdbc_url_template: str = 'jdbc:{driver}://{host}:{port}/{db_name}?tinyInt1isBit=false&zeroDateTimeBehavior=round'
    _sqoop_params: Dict[str, Optional[Union[str, int, bool]]] = {
        'hive-import': True,
        'hive-overwrite': True,
        'delete-target-dir': True,
        'autoreset-to-one-mapper': True,
        'null-string': "'\\\\N'",
        'null-non-string': "'\\\\N'",
        'hive-delims-replacement': '" \\\\\\\\$% "',
        'm': Config.get_config("sqoop_m")
    }

    @staticmethod
    def _get_jdbc_url_dict(setting: Setting) -> Dict[str, str]:
        url: str = SqoopCmd._jdbc_url_template.format(
            driver=setting.driver,
            host=setting.host,
            port=setting.port,
            db_name=setting.db_name)
        url_enclosed_with_double_quotes: str = '"{url}"'.format(url=url)
        return {'connect': url_enclosed_with_double_quotes}

    @staticmethod
    def _get_auth_dict(setting: Setting) -> Dict[str, str]:
        return {
            'username': setting.user,
            'password': setting.passwd
        }

    @staticmethod
    def _get_type_mapping_dict(
            map_columns_java: Optional[Dict[str, str]],
            map_columns_hive: Optional[Dict[str, str]]) -> Dict[str, str]:
        column_mapping_dict: Dict[str, str] = {}
        if map_columns_java:
            java_mapping_string: str = SqoopCmd.__get_type_mapping_string(map_columns_java)
            column_mapping_dict['map-column-java']: str = java_mapping_string
        if map_columns_hive:
            hive_mapping_string: str = SqoopCmd.__get_type_mapping_string(map_columns_hive)
            column_mapping_dict['map-column-hive']: str = hive_mapping_string
        return column_mapping_dict

    @staticmethod
    def __get_type_mapping_string(type_mapping_dict: Dict[str, str]) -> str:
        type_mapping_list: List[str] = ['{name}={type}'.format(name=name, type=type) for name, type in
                                        type_mapping_dict.items()]
        return ','.join(type_mapping_list)

    @staticmethod
    def _get_select_columns_dict(columns: Optional[List[str]]) -> Dict[str, str]:
        if columns:
            select_columns_string: str = ', '.join(columns)
            select_columns_dict: Dict[str, str] = {
                'columns': '"' + select_columns_string + '"'} if select_columns_string else {}
            return select_columns_dict
        else:
            return {}

    @staticmethod
    def _get_table_dict(src_db_table_name: Tuple[str, str], dst_db_table_name: Tuple[str, str]) -> Dict[str, str]:
        # if columnar table, use temp table for hive
        src_db_name, src_table_name = src_db_table_name
        table: str = src_table_name

        dst_db_name, dst_table_name = dst_db_table_name
        hive_table: str = "{dst_db_name}.{dst_table_name}".format(dst_db_name=dst_db_name,
                                                                  dst_table_name=dst_table_name)

        table_dict: Dict[str, str] = {
            'table': table,
            'hive-table': hive_table
        }

        return table_dict

    @staticmethod
    def __arg2str(key: str, value: Any) -> str:
        prefix: str = '-' if len(key) == 1 else '--'
        value: Any = '' if isinstance(value, bool) else value
        return '{0}{1} {2}'.format(prefix, key, value).strip()

    @staticmethod
    def get_cmd(
            src_db_table_name: Tuple[str, str],
            dst_db_table_name: Tuple[str, str],
            columns: Optional[List[str]] = None,
            map_columns_java: Optional[Dict[str, str]] = None,
            map_columns_hive: Optional[Dict[str, str]] = None) -> str:
        my_dict: Dict[str, Optional[Union[str, int, bool]]] = SqoopCmd._sqoop_params
        setting: Setting = Setting.load(src_db_table_name[0])

        # mandatory params
        my_dict: Dict[str, Optional[Union[str, int, bool]]] = merge_dict(my_dict,
                                                                         SqoopCmd._get_table_dict(src_db_table_name,
                                                                                                  dst_db_table_name))
        my_dict: Dict[str, Optional[Union[str, int, bool]]] = merge_dict(my_dict, SqoopCmd._get_jdbc_url_dict(setting))
        my_dict: Dict[str, Optional[Union[str, int, bool]]] = merge_dict(my_dict, SqoopCmd._get_auth_dict(setting))

        # optional params
        my_dict: Dict[str, Optional[Union[str, int, bool]]] = merge_dict(my_dict,
                                                                         SqoopCmd._get_type_mapping_dict(
                                                                             map_columns_java,
                                                                             map_columns_hive))
        my_dict: Dict[str, Optional[Union[str, int, bool]]] = merge_dict(my_dict,
                                                                         SqoopCmd._get_select_columns_dict(columns))

        # create cmd
        sqoop_cmd_body: str = ' \\\n  '.join([SqoopCmd.__arg2str(key, value) for key, value in my_dict.items()])
        sqoop_cmd: str = SqoopCmd._sqoop_cmd_prefix + ' \\\n  ' + sqoop_cmd_body

        return sqoop_cmd
