from typing import Dict, List, Any
from unittest import TestCase

from src.main.company.utils.dict_utils import merge_dict


class DictUtilsTest(TestCase):

    def test_merge_dict(self) -> None:
        dict_a_seq_in: List[Dict[Any, Any]] = [
            {
                'k1': 'v1',
                'k3': 'v3',
                'k5': 'v5'
            },
            {
                'k1': 'v1',
                'k3': 'v3',
                'k5': 'v5'
            },
            {
                'k1': {
                    'k1': 'v1',
                    'k3': 'v3',
                    'k5': 'v5'
                },
                'k2': 'v2'
            },
            {
                'k1': 'v1',
                'k2': [1, 3, 5, 7],
                'k3': 'v3'
            }
        ]
        dict_b_seq_in: List[Dict[Any, Any]] = [
            {
                'k2': 'v2',
                'k4': 'v4',
                'k6': 'v6'
            },
            {
                'k2': 'v2',
                'k4': 'v4',
                'k1': 'v6'
            },
            {
                'k1': {
                    'k2': 'v2',
                    'k4': 'v4',
                    'k6': 'v6'
                },
                'k3': 'v3'
            },
            {
                'k0': 'v0',
                'k2': [2, 3, 6, 7, 8],
                'k4': 'v4'
            }
        ]

        dict_merged_seq_out_expected: List[Dict[Any, Any]] = [
            {
                'k1': 'v1',
                'k3': 'v3',
                'k5': 'v5',
                'k2': 'v2',
                'k4': 'v4',
                'k6': 'v6'
            },
            {
                'k1': 'v1',
                'k3': 'v3',
                'k5': 'v5',
                'k2': 'v2',
                'k4': 'v4'
            },
            {
                'k1': {
                    'k1': 'v1',
                    'k3': 'v3',
                    'k5': 'v5',
                    'k2': 'v2',
                    'k4': 'v4',
                    'k6': 'v6'
                },
                'k2': 'v2',
                'k3': 'v3'
            },
            {
                'k1': 'v1',
                'k2': [1, 3, 5, 7, 2, 6, 8],
                'k3': 'v3',
                'k0': 'v0',
                'k4': 'v4'
            }
        ]
        dict_merged_seq_out_computed: List[Dict[Any, Any]] = [merge_dict(dict_a_seq_in[i], dict_b_seq_in[i]) for i in
                                                              range(len(dict_a_seq_in))]

        for i in range(len(dict_a_seq_in)):
            self.assertDictEqual(dict_merged_seq_out_expected[i], dict_merged_seq_out_computed[i])
