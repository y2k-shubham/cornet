from src.main.company.config.config import Config
from src.main.company.drivers.invokers.base_invoker import BaseInvoker


class ParallelInvoker(BaseInvoker):

    def __init__(self, **kwargs) -> None:
        num_instances: int = len(set(Config.get_db_instance_map().values()))
        super(ParallelInvoker, self).__init__(pool_size=num_instances, **kwargs)


def main() -> None:
    parallel_invoker: ParallelInvoker = ParallelInvoker(file_paths=[
        '/home/hadoop/y2k-shubham/task_configs/task_configs_1.json',
        '/home/hadoop/y2k-shubham/task_configs/task_configs_2.json',
        '/home/hadoop/y2k-shubham/task_configs/task_configs_3.json'
    ])
    parallel_invoker.process_task_configs(show=True, execute=False)
    #parallel_invoker.show_failed_tables()
    #parallel_invoker.show_table_metrics()
    #parallel_invoker.show_db_metrics()


if __name__ == '__main__':
    main()
