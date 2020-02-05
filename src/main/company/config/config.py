import json
from typing import Any, Dict, Optional, List

from src.main.company.config.environment import Environment
from src.main.company.utils.dict_utils import dict_get_value_recursively


class Config:

    @staticmethod
    def get_config_file_path() -> str:
        return 'src/main/resources/config/config.json'

    @staticmethod
    def get_config(path: str) -> Any:
        with open(Config.get_config_file_path()) as config_file:
            config_dict: Dict[str, Any] = json.load(config_file)
            return Config._get_config(config_dict[Environment.get_environment()], path)

    @staticmethod
    def get_db_instance_map() -> Dict[str, str]:
        return Config.get_config("db_instance_map")

    @staticmethod
    def get_db_rank(db_name: str) -> Optional[int]:
        db_names: List[str] = list(Config.get_db_instance_map().keys())
        if db_name in db_names:
            return db_names.index(db_name)
        else:
            return None

    @staticmethod
    def _get_config(my_dict: Dict[str, Any], path: str) -> Any:
        return dict_get_value_recursively(my_dict, path)
