from typing import Dict, Any

from src.main.company.connectors.base_connector import BaseConnector


def test_get_password_from_string():
    source: Dict[str, Any] = {'password': '123'}
    assert BaseConnector(source)._get_password() == '123'


def test_get_password_from_file():
    source: Dict[str, Any] = {'password_file': 'test/configs/password'}
    assert BaseConnector(source)._get_password() == 'my-secret-password-123'
