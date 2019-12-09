import unittest
from unittest.mock import patch
import table_alias_file


class TestTableAliasFile(unittest.TestCase):

    def test_update_contents(self):
        old_contents = 'g1_2018_08_10,g2_2018_08_10,g3_2018_08_10,g4_2018_08_10\n'
        alias = 'g1_2018_08_11'
        new_contents = table_alias_file.update_contents(alias, old_contents)
        expected = ''.join([
            'g1_2018_08_11,g2_2018_08_10,g3_2018_08_10,g4_2018_08_10\n',
            'g1_2018_08_10,g2_2018_08_10,g3_2018_08_10,g4_2018_08_10\n'
        ])
        self.assertEquals(new_contents, expected)

    def test_update_contents_when_duplicate(self):
        old_contents = 'g1_2018_08_10,g2_2018_08_10,g3_2018_08_10,g4_2018_08_10\n'
        alias = 'g1_2018_08_10'
        new_contents = table_alias_file.update_contents(alias, old_contents)
        expected = 'g1_2018_08_10,g2_2018_08_10,g3_2018_08_10,g4_2018_08_10\n'
        self.assertEquals(new_contents, expected)

    @patch('table_alias_file.MAX_LINES', 2)
    def test_update_contents_when_dropping_lines(self):
        old_contents = ''.join([
            'g1_2018_08_11,g2_2018_08_10,g3_2018_08_10,g4_2018_08_10\n',
            'g1_2018_08_10,g2_2018_08_10,g3_2018_08_10,g4_2018_08_10\n'
        ])
        alias = 'g2_2018_08_11'
        new_contents = table_alias_file.update_contents(alias, old_contents)
        expected = ''.join([
            'g1_2018_08_11,g2_2018_08_11,g3_2018_08_10,g4_2018_08_10\n',
            'g1_2018_08_11,g2_2018_08_10,g3_2018_08_10,g4_2018_08_10\n'
        ])
        self.assertEquals(new_contents, expected)
