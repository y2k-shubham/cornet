from datetime import datetime
from typing import Dict, List, Optional

from src.main.company.config.config import Config
from src.main.company.metrics.db_metric import DbMetric
from src.main.company.metrics.table_metric import TableMetric
from src.main.company.models.table import Table


def group_table_metrics_by_db(table_metrics: List[TableMetric]) -> Dict[str, List[TableMetric]]:
    db_instance_map: Dict[str, str] = Config.get_db_instance_map()
    db_names: List[str] = list(db_instance_map.keys())

    grouped_table_metrics: Dict[str, List[TableMetric]] = {db_name: filter_table_metrics_by_db(table_metrics, db_name)
                                                           for db_name in db_names}

    # remove dbs for which no table_metrics are present
    for db_name in db_names:
        if db_name in grouped_table_metrics and not grouped_table_metrics[db_name]:
            grouped_table_metrics.pop(db_name)

    return grouped_table_metrics


def create_db_metric(table_metrics: List[TableMetric], db_name: Optional[str] = None) -> Optional[DbMetric]:
    if table_metrics:
        db_name: str = db_name if db_name else table_metrics[0].db.name
        filtered_table_metrics: List[TableMetric] = filter_table_metrics_by_db(table_metrics, db_name)
        failed_table_metrics: List[TableMetric] = filter_table_metrics_by_failure(filtered_table_metrics)

        # params
        failed_tables: List[Table] = [table_metric.table for table_metric in failed_table_metrics]
        db_start_time: datetime = min([table_metric.start_time for table_metric in filtered_table_metrics])
        db_end_time: datetime = max([table_metric.end_time for table_metric in filtered_table_metrics])

        # db_metric
        db_metric: DbMetric = DbMetric(
            db=filtered_table_metrics[0].db,
            start_time=db_start_time,
            end_time=db_end_time
        )
        db_metric.failed_tables: List[Table] = failed_tables

        return db_metric


def filter_table_metrics_by_failure(table_metrics: List[TableMetric], failed: bool = True) -> List[TableMetric]:
    if failed:
        return [table_metric for table_metric in table_metrics if table_metric.did_fail()]
    else:
        return [table_metric for table_metric in table_metrics if not table_metric.did_fail()]


def filter_table_metrics_by_db(table_metrics: List[TableMetric], db_name: str) -> List[TableMetric]:
    filtered_table_metrics: List[TableMetric] = [table_metric for table_metric in table_metrics if
                                                 table_metric.db.name == db_name]
    return filtered_table_metrics
