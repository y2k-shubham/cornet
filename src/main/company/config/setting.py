import json
from typing import Dict, Optional, Union

from src.main.company.config.config import Config
from src.main.company.models.db import Db

"""
    "settings": {
        "db": "db_name",
        "host": "host_url",
        "user": "user_name",
        "password": "password",
        "port": 3306,
        "driver": "mysql"
    }
"""


class Setting:

    def __init__(
            self,
            db_name: str,
            host: str,
            user: str,
            passwd: str,
            port: int = 3306,
            driver: str = 'mysql') -> None:
        self.db_name: str = db_name
        self.host: str = host
        self.user: str = user
        self.passwd: str = passwd
        self.port: int = port
        self.driver: str = driver

    @staticmethod
    def load(db: Union[Db, str]) -> 'Setting':
        # templates
        db_name: str = db.name if isinstance(db, Db) else db
        settings_file_path_template: str = Config.get_config("credentials_directory") + '/{db_name}.json'

        # parameters
        settings_file_path: str = settings_file_path_template.format(db_name=db_name)

        # read settings
        with open(settings_file_path) as settings_file:
            settings_dict: Dict[str, Optional[Union[str, int]]] = json.load(settings_file)

        return Setting(
            db_name=settings_dict['database'],
            host=settings_dict['host'],
            user=settings_dict['user'],
            passwd=settings_dict['password'],
            port=settings_dict.get('port', 3306),
            driver=settings_dict.get('driver', 'mysql')
        )
