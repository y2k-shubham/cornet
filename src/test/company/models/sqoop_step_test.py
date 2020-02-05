from unittest import TestCase

from src.main.company.models.sqoop_step import SqoopStep


class SqoopStepTest(TestCase):

    def test__init__(self) -> None:
        sqoop_step: SqoopStep = SqoopStep(
            map_columns_hive={
                'k1': 'v1',
                'k2': 'v2'
            }
        )
        print(sqoop_step)
