"""
Tests for version.py.
"""

import unittest
from unittest.mock import Mock

from logging import Logger

from ..version import get_versions, copy_versions_to_dbfs
from ..services import GitPath, GitHub, DBFS

class TestVersionHelpers(unittest.TestCase):
    """
    Tests for version helper functions.
    """

    def test_get_versions(self):
        """
        Test the extration of versions from different paths.
        """
        # Arrange
        base_paths = ['base_path', 'base/path']

        for base_path in base_paths:
            with self.subTest(f'Base Path = {base_path}'):
                # Valid paths should look like {base_path}/{version}/{file}.
                files = [
                    f'{base_path}/v0.0.1/file1.csv',
                    f'{base_path}/v0.0.1/file2.csv',
                    f'{base_path}/v0.0.2/file2.csv',
                    f'{base_path}/file2.csv',
                    f'{base_path}/another_path/v0.0.3/file2.csv',
                    'another_path/v0.0.4/file1.csv',
                    f'v0.0.5/{base_path}/file2.csv']

                # Act
                results = get_versions(base_path, files)

                # Assert
                expected_results = set(['v0.0.2', 'v0.0.1'])
                self.assertSetEqual(expected_results, results)

    def test_copy_versions_to_dbfs(self):
        """
        Test the copy of all files in version folders from GitHub to Databricks
        """
        # Arrange
        versions = ['v0.0.1', 'v0.0.2']
        mock_git = Mock(spec = GitHub)
        repo = 'magencio/git_to_dbfs_function'
        base_path = 'samplefiles'
        branch = 'master'
        git_base_path = GitPath(repo, base_path, branch)
        mock_dbfs = Mock(spec = DBFS)
        dbfs_base_path = '/mnt/playground/magencio/data/samplefiles'

        logger = Mock(spec=Logger)

        # Act
        copy_versions_to_dbfs(versions, mock_git, git_base_path, mock_dbfs, dbfs_base_path, logger)

        # Assert
        expected_args = [
            (repo, f'{base_path}/v0.0.1', branch, mock_dbfs, f'{dbfs_base_path}/v0.0.1'),
            (repo, f'{base_path}/v0.0.2', branch, mock_dbfs, f'{dbfs_base_path}/v0.0.2')]
        args = [
            (x[0][0].repo, x[0][0].path, x[0][0].branch, x[0][1], x[0][2])
            for x in mock_git.copy_folder_to_dbfs.call_args_list]

        self.assertCountEqual(expected_args, args)
