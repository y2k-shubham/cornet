from contextlib import closing
from typing import Tuple, Dict, List, Set, Optional, Union

from MySQLdb import connect

from src.main.company.config.setting import Setting
from src.main.company.utils.dict_utils import merge_dict

# mapping between MySQL datatypes and Hive datatypes (not exhaustive)
mysql_hive_type_mappings: Dict[str, str] = {
    'bigint': 'bigint',
    'bit': 'boolean',
    'blob': 'binary',
    'char': 'string',
    'date': 'date',
    'datetime': 'timestamp',
    'decimal': 'decimal',
    'double': 'double',
    'enum': 'string',
    'float': 'float',
    'int': 'int',
    'longtext': 'string',
    'mediumint': 'int',
    'mediumtext': 'string',
    'smallint': 'smallint',
    'text': 'string',
    'time': 'string',
    'timestamp': 'timestamp',
    'tinyint': 'tinyint',
    'tinytext': 'string',
    'varbinary': 'binary',
    'varchar': 'string'
}

# set of Hive datatypes (not exhaustive)
hive_types: Set[str] = {
    'boolean',
    'tinyint',
    'smallint',
    'int',
    'bigint',
    'float',
    'double',
    'decimal',
    'date',
    'timestamp',
    'string',
    'binary'
}


def get_mysql_columns(db_table_name: Tuple[str, str]) -> Dict[str, str]:
    """
    Accepts a table name and returns MySQL schema of that table
    Schema consists a dictionary mapping column names to datatypes

    - For all types of column except decimal, the value contains only
      name of datatype (without field length)
    - For decimal type columns, the value also contains width and precision
      in parenthesis (as usual)
    :param db_table_name:   Name of table qualified with db name
    :type db_table_name:    str
    :return:                schema of given MySQL table
    :type:                  Dict[str, str]
    """
    sql_query: str = """
            SELECT \
                `column_name`, \
                `data_type`, \
                `column_type` \
            FROM \
                `information_schema`.`columns` \
            WHERE \
                `table_schema` = '{db_name}' \
                AND `table_name` = '{table_name}'
        """.format(db_name=db_table_name[0], table_name=db_table_name[1])
    setting: Setting = Setting.load(db=db_table_name[0])
    with closing(connect(
            host=setting.host,
            port=setting.port,
            user=setting.user,
            passwd=setting.passwd,
            db=setting.db_name)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(sql_query)
            result_set: List[Tuple[str, str, str]] = cursor.fetchall()
            mysql_schema: Dict[str, str] = {row[0]: (row[1] if row[1] != 'decimal' else row[2].split(' ')[0])
                                            for row in result_set}
            return mysql_schema


def get_resultant_schema(
        existing_schema: Dict[str, str],
        select_columns: Optional[List[str]] = None,
        drop_selected: bool = True) -> Dict[str, str]:
    """
    Accepts an existing schema of columns and an optional list of columns
    and returns the resultant schema, based on drop_selected flag

    If drop_selected flag is True (default), the list of select_columns is
    dropped from existing schema while the remaining columns are picked;
    otherwise select_columns are picked and remaining columns are dropped
    :param existing_schema: Schema of existing set of columns
    :type existing_schema:  Dict[str, str] (column_name -> column_type)
    :param select_columns:  Optional list of columns (names) to be picked or dropped
                            from existing schema, based on flag drop_selected
    :type select_columns:   Optional[List[str]]
    :param drop_selected:   Flag controlling whether select_columns are picked
                            from existing schema (and rest are dropped) or vice-versa
    :type drop_selected:    bool (default True)
    :return:                Resulting schema after picking / dropping select_columns
                            from existing columns
    :type:                  Dict[str, str]
    """
    if select_columns:
        if drop_selected:
            return {column_name: column_type for column_name, column_type in existing_schema.items() if
                    column_name not in select_columns}
        else:
            return {column_name: column_type for column_name, column_type in existing_schema.items() if
                    column_name in select_columns}
    else:
        return existing_schema


def get_mysql_schema(
        db_table_name: Tuple[str, str],
        select_columns: Optional[List[str]] = None,
        drop_selected: bool = True) -> Dict[str, str]:
    """
    Accepts a table-name and optional list of select columns (to be
    picked / dropped) and returns the resultant schema of MySQL columns
    :param db_table_name:   Name of table qualified with db-name
    :type db_table_name:    str
    :param select_columns:  (Optional) list of columns to be selected or dropped
    :type select_columns:   Optional[List[str]]
    :param drop_selected:   (Optional) flag controlling whether select_columns
                            are picked or dropped from origin MySQL schema
    :type drop_selected:    bool (default True)
    :return:                Resultant schema of MySQL columns to be picked
    :type:                  Dict[str, str]
    """
    existing_schema: Dict[str, str] = get_mysql_columns(db_table_name)
    return get_resultant_schema(
        existing_schema=existing_schema,
        select_columns=select_columns,
        drop_selected=drop_selected
    )


def get_hive_schema(
        existing_schema: Dict[str, str],
        select_columns: Optional[List[str]] = None,
        drop_selected: bool = True,
        add_columns: Optional[Union[Dict[str, str], List[Dict[str, str]]]] = None) -> Dict[str, str]:
    """
    Given an existing schema to use and (optional) list of columns to pick / drop
    (from existing schema) and (optional) schema of new columns to be added, returns
    a resulting schema of Hive columns

    if drop_selected flag is True (default), the selected_columns are dropped from
    existing schema (and the rest are picked), otherwise the selected_columns are
    picked from existing schema (and the rest are dropped)

    existing schema could have column types either of MySQL or Hive
    returned schema always has Hive types
    :param existing_schema: Existing schema on which to build upon the Hive schema
                             Could have either MySQL datatypes or Hive datatypes
    :type existing_schema:  Dict[str, str]
    :param select_columns: (Optional) list of columns to be picked from existing schema
                             ignored if it is empty list
    :type select_columns:  Optional[List[str]]
    :param drop_selected:    (Optional) flag controlling whether selected columns are to be
                             picked or dropped
    :type drop_selected:     bool (default: True)
    :param add_columns:      (Optional) schema of columns to be added to existing schema
                             Must contain either MySQL or Hive datatypes

                             it could be either of following
                             - a dictionary of schema (just like existing_columns)
                             - a list of dictionaries, where each dictionary holds information
                               of one column; keys 'name' and 'type' are extracted from each
                               dictionary to obtain schema of columns to be added
    :type add_columns:       Dict[str, str] or List[Dict[str, str]]
    :return:                 Resulting Hive schema
    :type:                   Dict[str, str]
    """
    pruned_schema: Dict[str, str] = get_resultant_schema(
        existing_schema=existing_schema,
        select_columns=select_columns,
        drop_selected=drop_selected
    )

    if isinstance(add_columns, List):
        columns_to_add: Dict[str, str] = {column_dict['name']: column_dict['type'] for column_dict in add_columns}
    elif isinstance(add_columns, Dict):
        columns_to_add: Dict[str, str] = add_columns
    else:
        columns_to_add: Dict[str, str] = {}
    all_columns: Dict[str, str] = merge_dict(pruned_schema, columns_to_add)
    all_columns_hive_type: Dict[str, str] = {column_name: convert_to_hive_type(column_type) for column_name, column_type
                                             in all_columns.items()}

    return all_columns_hive_type


def convert_to_hive_type(datatype: str) -> str:
    """
    Accepts name of a datatype (str) and returns
    - Closest Hive datatype name if input type is of MySQL
    - Same datatype name as input if input type if of Hive
    - 'string' if input is none of the above

    In other words, the return value is guaranteed to be
    a valid Hive datatype name
    :param datatype: Input datatype name (MySQL / Hive / other)
    :type datatype:  str
    :return:         Closest matching Hive datatype name for the given input
    :type:           str
    """
    hive_type: str = None
    if datatype.startswith("decimal"):
        hive_type: str = datatype
    elif datatype in mysql_hive_type_mappings:
        hive_type: str = mysql_hive_type_mappings[datatype]
    elif datatype in hive_types:
        hive_type: str = datatype
    else:
        hive_type: str = 'string'
    return hive_type


def extract_select_stmt_for_column(
        column_name: str,
        schema: Union[List[Dict[str, str]], Dict[str, Tuple[str, Optional[str]]]]) -> Optional[str]:
    select_stmt: Optional[str] = None

    if isinstance(schema, List):
        for column_dict in schema:
            if (column_dict.get('name', None) == column_name) and ('select_stmt' in column_dict):
                select_stmt: str = column_dict['select_stmt']
                break
    elif isinstance(schema, Dict) and (column_name in schema) and isinstance(schema[column_name], Tuple):
        select_stmt_opt: Optional[str] = schema[column_name][1]
        if select_stmt_opt:
            select_stmt: str = select_stmt_opt

    return select_stmt
