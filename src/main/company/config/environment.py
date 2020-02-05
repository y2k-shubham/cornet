import json
from typing import Dict, Any


class Environment:

    @staticmethod
    def get_environment_path() -> str:
        return 'src/main/resources/config/environment.json'

    @staticmethod
    def get_environment() -> str:
        try:
            with open(Environment.get_environment_path()) as environment_file:
                environment_dict: Dict[str, Any] = json.load(environment_file)
                return environment_dict.get("environment", "development")
        except:
            return "development"
