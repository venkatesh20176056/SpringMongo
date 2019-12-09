import unittest
import itertools
from unittest.mock import MagicMock, patch
import crawler


class TestCrawler(unittest.TestCase):

    def test_load_template(self):
        res = crawler.load_template('group_exporter')
        self.assertEqual(type(res), str)

    def test_assert_crawler_target_exists(self):
        template = crawler.load_template('group_exporter')
        with patch('crawler.assert_s3_dir_exists') as mock:
            crawler.assert_crawler_target_exists(template)

    def test_substitute(self):
        template = '{ "${key1}": "${key2}" }'
        replacements = {
            'key1': 'value1',
            'key2': 'value2',
        }
        expected = '{ "value1": "value2" }'
        self.assertEqual(crawler.substibute(template, **replacements), expected)

    def test_ensure_deleted_when_deleted(self):
        with patch('crawler.remote_status') as mock_status:
            mock_status.return_value = None
            crawler.ensure_deleted('deleted_crawler')

    def test_ensure_deleted_when_ready(self):
        with patch('crawler.remote_delete') as mock_delete:
            with patch('crawler.remote_status') as mock_status:
                mock_status.side_effect = [crawler.State.READY, None]
                crawler.ensure_deleted('finished_crawler')
            mock_delete.assert_called_once_with('finished_crawler')

    def test_ensure_deleted_when_running(self):
        with patch('crawler.remote_status') as mock_status:
            mock_status.return_value = crawler.State.RUNNING
            with self.assertRaises(AssertionError):
                crawler.ensure_deleted('running_crawler')

    def test_wait_for_completion_when_completes(self):
        with patch('crawler.remote_status') as mock_status:
            status_values = [
                crawler.State.RUNNING,
                crawler.State.RUNNING,
                crawler.State.STOPPING,
                crawler.State.READY
            ]
            mock_status.side_effect = status_values
            crawler.wait_for_completion('test_crawler', 0.001, 10)
            self.assertEqual(mock_status.call_count, len(status_values))

    def test_wait_for_completion_when_timeout(self):
        with patch('crawler.remote_status') as mock_status:
            mock_status.side_effect = itertools.repeat(crawler.State.RUNNING)
            with self.assertRaises(TimeoutError):
                crawler.wait_for_completion('test_crawler', 0.001, 0.01)
