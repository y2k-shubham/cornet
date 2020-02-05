from unittest import TestCase

from src.main.company.utils.schema_utils import *


class SchemaUtilsTest(TestCase):

    def test_resultant_schema(self) -> None:
        existing_schema_seq_in: List[Dict[str, str]] = [
                                                           {
                                                               "column_name_1": "type_name_1",
                                                               "column_name_2": "type_name_2",
                                                               "column_name_3": "type_name_3"
                                                           }
                                                       ] * 4 + [
                                                           {'id': 'bigint', 'invoice_id': 'bigint',
                                                            'service_type': 'enum', 'sub_service': 'varchar',
                                                            'property': 'varchar', 'value': 'mediumtext',
                                                            'deleted': 'tinyint'}
                                                       ]
        select_columns_seq_in: List[Optional[List[str]]] = [
            [
                "column_name_1",
                "column_name_3"
            ],
            [
                "column_name_1",
                "column_name_3"
            ],
            [],
            None,
            ['id', 'invoice_id', 'service_type']
        ]
        drop_selected_seq_int: List[bool] = [
            False,
            True,
            False,
            False,
            False
        ]

        resultant_schema_seq_out_expected: List[Dict[str, str]] = [
            {
                "column_name_1": "type_name_1",
                "column_name_3": "type_name_3"
            },
            {
                "column_name_2": "type_name_2"
            },
            {
                "column_name_1": "type_name_1",
                "column_name_2": "type_name_2",
                "column_name_3": "type_name_3"
            },
            {
                "column_name_1": "type_name_1",
                "column_name_2": "type_name_2",
                "column_name_3": "type_name_3"
            },
            {
                'id': 'bigint',
                'invoice_id': 'bigint',
                'service_type': 'enum'
            }
        ]
        resultant_schema_seq_out_computed: List[Dict[str, str]] = [
            get_resultant_schema(existing_schema_seq_in[i], select_columns_seq_in[i], drop_selected_seq_int[i])
            for i in range(len(existing_schema_seq_in))
        ]

        for i in range(len(existing_schema_seq_in)):
            self.assertDictEqual(resultant_schema_seq_out_expected[i], resultant_schema_seq_out_computed[i])

    def test_get_hive_schema(self) -> None:
        existing_schema_seq_in: List[Dict[str, str]] = [
                                                           {
                                                               "column_name_1": "bit",
                                                               "column_name_2": "tinyint",
                                                               "column_name_3": "smallint"
                                                           }
                                                       ] * 5 + [
            {'id': 'bigint', 'invoice_id': 'bigint',
             'service_type': 'enum', 'sub_service': 'varchar',
             'property': 'varchar', 'value': 'mediumtext',
             'deleted': 'tinyint'}
        ]
        select_columns_seq_in: List[Optional[List[str]]] = [
            None,
            None,
            ["column_name_2"],
            ["column_name_1", "column_name_3"],
            [],
            ['id', 'invoice_id', 'service_type']
        ]
        drop_selected_seq_in: List[bool] = [
            False,
            False,
            True,
            False,
            True,
            False
        ]
        add_columns_seq_in: List[Optional[List[Dict[str, str]]]] = [
            None,
            [
                {
                    "name": "added_column_name_1",
                    "type": "mediumint",
                    "select_stmt": "added_column_1-select_stmt"
                }
            ],
            [
                {
                    "name": "added_column_name_1",
                    "type": "mediumint",
                    "select_stmt": "added_column_1-select_stmt"
                },
                {
                    "name": "added_column_name_2",
                    "type": "datetime",
                    "select_stmt": "added_column_2-select_stmt"
                }
            ],
            [
                {
                    "name": "added_column_name_1",
                    "type": "date",
                    "select_stmt": "added_column_1-select_stmt"
                }
            ],
            [],
            None
        ]

        hive_schema_seq_out_expected: List[Dict[str, str]] = [
            {
                "column_name_1": "boolean",
                "column_name_2": "tinyint",
                "column_name_3": "smallint"
            },
            {
                "column_name_1": "boolean",
                "column_name_2": "tinyint",
                "column_name_3": "smallint",
                "added_column_name_1": "int"
            },
            {
                "column_name_1": "boolean",
                "column_name_3": "smallint",
                "added_column_name_1": "int",
                "added_column_name_2": "timestamp"
            },
            {
                "column_name_1": "boolean",
                "column_name_3": "smallint",
                "added_column_name_1": "date"
            },
            {
                "column_name_1": "boolean",
                "column_name_2": "tinyint",
                "column_name_3": "smallint"
            },
            {
                'id': 'bigint',
                'invoice_id': 'bigint',
                'service_type': 'string'
            }
        ]
        hive_schema_seq_out_computed: List[Dict[str, str]] = [
            get_hive_schema(
                existing_schema_seq_in[i],
                select_columns_seq_in[i],
                drop_selected_seq_in[i],
                add_columns_seq_in[i]
            ) for i in range(len(existing_schema_seq_in))
        ]

        for i in range(len(existing_schema_seq_in)):
            self.assertDictEqual(hive_schema_seq_out_expected[i], hive_schema_seq_out_computed[i])
