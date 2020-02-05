from src.main.company.drivers.invokers.base_invoker import BaseInvoker


class SerialInvoker(BaseInvoker):

    def __init__(self, **kwargs) -> None:
        super(SerialInvoker, self).__init__(pool_size=1, **kwargs)


def main() -> None:
    serial_invoker: SerialInvoker = SerialInvoker(file_paths=[
        '/home/hadoop/y2k-shubham/task_configs/task_configs_1.json',
        '/home/hadoop/y2k-shubham/task_configs/task_configs_2.json',
        '/home/hadoop/y2k-shubham/task_configs/task_configs_3.json'
    ])
    serial_invoker.process_task_configs(show=True, execute=False)
    #serial_invoker.show_failed_tables()
    #serial_invoker.show_table_metrics()
    #serial_invoker.show_db_metrics()


if __name__ == '__main__':
    main()
