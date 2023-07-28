import pytest

from data_qaqc_auto_run import DataQAQCAutoRunHandler
from datetime import datetime as dt
from jira_interface import JIRAInterface

__author__ = 'You-Wei Cheah'
__email__ = 'ycheah@lbl.gov'


@pytest.fixture
def handler(monkeypatch):
    def mock_jira_get_organization(jira_instance):
        return {
            'US-Ton': '207',
            'BR-Sa1': '78',
            'BR-Sa3': '103',
            'CA-Gro': '187'}

    monkeypatch.setattr(
        JIRAInterface, 'get_organizations', mock_jira_get_organization)

    return DataQAQCAutoRunHandler()


def test_create_db_entry(handler, monkeypatch):
    test_cases = {
        1: (None,) * 3,
        2: (123, 'Foo', 'Bar')
    }

    test_results = [
        handler._create_db_entry(*v) for _, v in test_cases.items()]
    for test_entry, result in zip(test_cases.items(), test_results):
        _, test_case = test_entry
        assert tuple(list(result[:2]) + list(result[-1:])) == test_case
        assert isinstance(result[2], dt)


def test_create_jira_link(handler, monkeypatch):
    jira_base = f'{handler.jira_host}{handler.jira_issue_path}'
    tests = (123, 1e6)
    for t in tests:
        t = int(t)
        assert handler._create_jira_link(t) == f'{jira_base}QAQC-{t}'

    tests = ('TESTQAQC-123', 'QAQC-123', 'FOO-987')
    for t in tests:
        project, issue = t.split('-')
        assert handler._create_jira_link(t) == f'{jira_base}{project}-{issue}'

    tests = ('TESTQAQC-123-123', '', None, '1e6', 'None-None')
    for t in tests:
        with pytest.raises(Exception):
            assert handler._create_jira_link(t)
