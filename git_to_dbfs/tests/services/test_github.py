"""
Tests for github.py.
"""

import unittest
from unittest.mock import Mock, patch, call

from hashlib import sha1
from hmac import HMAC
from logging import Logger
from typing import Callable

import azure.functions as func

from ...services import validate_payload, GitPath, GitPushNotification, GitHub, GitHubException
from ...services import DBFS, DBFSException

class TestGitHubHelpers(unittest.TestCase):
    """
    Tests for GitHub helper functions.
    """

    def test_validate_payload(self):
        """
        Test the validation of a GitHub Webhook payload.
        """
        # Arrange
        url = 'https://0bcb332ca10a.eu.ngrok.io/api/git_to_dbfs'
        encoded_body = 'somebody'.encode()
        secret = 'somesecret'
        encoded_secret = secret.encode()
        signature = HMAC(key=encoded_secret, msg=encoded_body, digestmod=sha1).hexdigest()
        headers = {'X-Hub-Signature': f'sha1={signature}'}
        req = func.HttpRequest(method='POST', url=url, headers=headers, body=encoded_body)

        # Act
        result = validate_payload(req, secret)

        # Assert
        self.assertTrue(result)

    def test_validate_payload_no_x_hub_signature(self):
        """
        Test the validation of a GitHub Webhook payload from a request without X-Hub-Signature.
        """
        # Arrange
        url = 'https://0bcb332ca10a.eu.ngrok.io/api/git_to_dbfs'
        encoded_body = 'somebody'.encode()
        secret = 'somesecret'
        req = func.HttpRequest(method='POST', url=url, body=encoded_body)

        # Act
        result = validate_payload(req, secret)

        # Assert
        self.assertFalse(result)

    def test_validate_payload_invalid_x_hub_signature_format(self):
        """
        Test the validation of a GitHub Webhook payload from a request with invalid X-Hub-Signature
        format.
        """
        # Arrange
        url = 'https://0bcb332ca10a.eu.ngrok.io/api/git_to_dbfs'
        encoded_body = 'somebody'.encode()
        secret = 'somesecret'
        encoded_secret = secret.encode()
        signature = HMAC(key=encoded_secret, msg=encoded_body, digestmod=sha1).hexdigest()
        headers = {'X-Hub-Signature': signature}
        req = func.HttpRequest(method='POST', url=url, headers=headers, body=encoded_body)

        # Act
        result = validate_payload(req, secret)

        # Assert
        self.assertFalse(result)

    def test_validate_payload_invalid_signature(self):
        """
        Test the validation of a GitHub Webhook payload when secret is invalid.
        """
        # Arrange
        url = 'https://0bcb332ca10a.eu.ngrok.io/api/git_to_dbfs'
        encoded_body = 'somebody'.encode()
        secret = 'somesecret'
        headers = {'X-Hub-Signature': 'sha1=someothersignature'}
        req = func.HttpRequest(method='POST', url=url, headers=headers, body=encoded_body)

        # Act
        result = validate_payload(req, secret)

        # Assert
        self.assertFalse(result)

    def test_validate_payload_invalid_secret(self):
        """
        Test the validation of a GitHub Webhook payload when secret is invalid.
        """
        # Arrange
        url = 'https://0bcb332ca10a.eu.ngrok.io/api/git_to_dbfs'
        encoded_body = 'somebody'.encode()
        secret = 'somesecret'
        encoded_secret = secret.encode()
        signature = HMAC(key=encoded_secret, msg=encoded_body, digestmod=sha1).hexdigest()
        headers = {'X-Hub-Signature': f'sha1={signature}'}
        req = func.HttpRequest(method='POST', url=url, headers=headers, body=encoded_body)

        # Act
        result = validate_payload(req, 'someothersecret')

        # Assert
        self.assertFalse(result)

class TestGitPath(unittest.TestCase):
    """
    Tests for GitPath class.
    """

    def test_init(self):
        """
        Test the construction of a GitPath object.
        """
        # Arrange
        repo = 'magencio/git_to_dbfs_function'
        path = 'samplefiles'
        branch = 'master'

        # Act
        result = GitPath(repo, path, branch)

        # Assert
        self.assertEqual(repo, result.repo)
        self.assertEqual(path, result.path)
        self.assertEqual(branch, result.branch)
        self.assertEqual(f'refs/heads/{branch}', result.ref)

class TestGitPushNotification(unittest.TestCase):
    """
    Tests for GitPushNotification class.
    """

    def test_init(self):
        """
        Test the construction of a GitPushNotification object.
        """
        # Arrange
        notification = {
            'ref': 'refs/heads/master',
            'repository': {
                'full_name': 'magencio/git_to_dbfs_function'
            },
            'commits': [{}]
        }

        # Act
        result = GitPushNotification(notification)

        # Assert
        self.assertIsNotNone(result)

    def test_init_missing_attributes(self):
        """
        Test the construction of a GitPushNotification object when Push information is missing
        from notification payload.
        """
        # Arrange
        missing_attributes = ['ref', 'repository', 'commits']

        for missing_attribute in missing_attributes:
            with self.subTest(f'Missing attribute = {missing_attribute}'):
                notification = {
                    'ref': 'refs/heads/master',
                    'repository': {
                        'full_name': 'magencio/git_to_dbfs_function'
                    },
                    'commits': [{}]
                }
                notification.pop(missing_attribute)

                # Act & Assert
                with self.assertRaises(AttributeError):
                    GitPushNotification(notification)

    def test_get_modified_files(self):
        """
        Tests the extraction of all modified files under a certain path from a
        GitPushNotification object.
        """
        # Arrange
        branch = 'master'
        repo = 'magencio/git_to_dbfs_function'
        path = 'samplefiles'
        notification = {
            'ref': f'refs/heads/{branch}',
            'repository': { 'full_name': repo },
            'commits': [
                {
                    'added': [f'{path}/v0.0.1/added.csv'],
                    'removed': [f'{path}/v0.0.1/removed.csv'],
                    'modified': [f'{path}/v0.0.1/modified.csv']
                },
                {
                    'added': [f'{path}/v0.0.2/added.csv', 'someotherfolder/someotherfile.py'],
                    'removed': [f'{path}/v0.0.2/removed.csv', f'v0.0.2/{path}/removed.csv'],
                    'modified': [f'{path}/v0.0.2/modified.csv', f'{path}/unk/v0.0.2/modified.csv']
                }
            ]
        }

        git_notification = GitPushNotification(notification)
        git_path = GitPath(repo, path, branch)

        # Act
        result = git_notification.get_modified_files(git_path)

        # Assert
        expected_result = [
            f'{path}/v0.0.1/added.csv', f'{path}/v0.0.1/removed.csv',
            f'{path}/v0.0.1/modified.csv', f'{path}/v0.0.2/added.csv',
            f'{path}/v0.0.2/removed.csv', f'{path}/v0.0.2/modified.csv',
            f'{path}/unk/v0.0.2/modified.csv']
        self.assertCountEqual(expected_result, result)

    def test_get_modified_files_from_invalid_notification(self):
        """
        Tests the extraction of all modified files under a certain path from a
        GitPushNotification object.
        """
        # Arrange
        branch = 'master'
        repo = 'magencio/git_to_dbfs_function'
        path = 'samplefiles'
        notification = {
            'ref': f'refs/heads/{branch}',
            'repository': { 'full_name': repo },
            'commits': [{ 'added': [f'{path}/v0.0.1/added.csv'], 'removed': [], 'modified': [] }]
        }

        git_notification = GitPushNotification(notification)

        invalid_properties = ['branch', 'repo']

        for invalid_property in invalid_properties:
            with self.subTest(f'Invalid property = {invalid_property}'):
                if invalid_property == 'branch':
                    git_path = GitPath(repo, path, 'dummy')
                else:
                    git_path = GitPath('emerging-tech/dummy', path, branch)

                # Act
                result = git_notification.get_modified_files(git_path)

                # Assert
                self.assertCountEqual([], result)

class TestGitHub(unittest.TestCase):
    """
    Tests for TestGitHub class.
    """

    @patch('requests.get')
    def test_repos_content(self, mock_get):
        """
        Tests getting the contents of a file or directory in a repository.
        """
        # Arrange
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            {'download_url': 'https://raw.githubusercontent.com//magencio/git_to_dbfs_function/master/'
                'samplefiles/v0.0.1/file1.csv?token=ABACQB6ZHRWQUSV5N2EGKWK7L5YJC'},
            {'download_url': 'https://raw.githubusercontent.com//magencio/git_to_dbfs_function/master/'
                'samplefiles/v0.0.1/file2.csv?token=ABACQB6ZHRWQUSV5N2EGKWK7L5YJC'}
        ]

        repo = 'magencio/git_to_dbfs_function'
        path = 'samplefiles'
        branch = 'master'
        git_path = GitPath(repo, path, branch)

        api_url = 'https://api.github.com'
        token = 'token'
        logger = Mock(spec=Logger)
        git = GitHub(api_url, token, logger)

        # Act
        result = git.repos_content(git_path)

        # Assert
        self.assertCountEqual(mock_get.return_value.json.return_value, result)

        mock_get.assert_called_once_with(f'{api_url}/repos/{repo}/contents/{path}',
            headers={'Authorization': f'Bearer {token}'}, params={'ref': branch})

    @patch('requests.get')
    def test_repos_content_with_github_error(self, mock_get):
        """
        Tests trying to get the contents of a file or directory in a repository but getting a
        GitHub error back.
        """
        # Arrange
        mock_get.return_value.status_code = 401
        mock_get.return_value.__bool__.return_value = False

        repo = 'magencio/git_to_dbfs_function'
        path = 'samplefiles'
        branch = 'master'
        git_path = GitPath(repo, path, branch)

        api_url = 'https://api.github.com'
        token = 'token'
        logger = Mock(spec=Logger)
        git = GitHub(api_url, token, logger)

        # Act & Assert
        with self.assertRaisesRegex(GitHubException, '401'):
            git.repos_content(git_path)

        mock_get.assert_called_once_with(f'{api_url}/repos/{repo}/contents/{path}',
            headers={'Authorization': f'Bearer {token}'}, params={'ref': branch})

    @patch('requests.get')
    def test_download_file(self, mock_get):
        """
        Tests the download of a file in chunks.
        """
        # Arrange
        chunk1 = 'chunk1'.encode()
        chunk2 = 'chunk2'.encode()
        mock_get.return_value.__enter__.return_value.status_code = 200
        mock_get.return_value.__enter__.return_value.iter_content.return_value =\
            iter([chunk1, chunk2])

        mock_got_chunk = Mock()

        download_url = 'https://raw.githubusercontent.com//magencio/git_to_dbfs_function/master/' +\
            'samplefiles/v0.0.1/file2.csv?token=ABACQB6ZHRWQUSV5N2EGKWK7L5YJC'

        api_url = 'https://api.github.com'
        token = 'token'
        logger = Mock(spec=Logger)
        git = GitHub(api_url, token, logger)

        # Act
        git.download_file(download_url, got_chunk=mock_got_chunk)

        # Assert
        mock_got_chunk.assert_has_calls([call(chunk1), call(chunk2)])

        mock_get.assert_called_once_with(download_url, headers={'Authorization': f'Bearer {token}'},
            stream=True)

    @patch('requests.get')
    def test_download_file_with_github_error(self, mock_get):
        """
        Tests the download of a file in chunks.
        """
        # Arrange
        mock_get.return_value.__enter__.return_value.status_code = 401
        mock_get.return_value.__enter__.return_value.__bool__.return_value = False

        mock_got_chunk = Mock()

        download_url = 'https://raw.githubusercontent.com//magencio/git_to_dbfs_function/master/' +\
            'samplefiles/v0.0.1/file2.csv?token=ABACQB6ZHRWQUSV5N2EGKWK7L5YJC'

        api_url = 'https://api.github.com'
        token = 'token'
        logger = Mock(spec=Logger)
        git = GitHub(api_url, token, logger)

        # Act & Assert
        with self.assertRaisesRegex(GitHubException, '401'):
            git.download_file(download_url, got_chunk=mock_got_chunk)

        mock_get.assert_called_once_with(download_url, headers={'Authorization': f'Bearer {token}'},
            stream=True)

    def test_copy_folder_to_dbfs(self):
        """
        Tests a copy of all files in a folder to a DBFS folder, after deleting all previous
        contents of the DBFS folder.
        """
        # Arrange
        repo = 'magencio/git_to_dbfs_function'
        path = 'samplefiles/v0.0.1'
        branch = 'master'
        git_path = GitPath(repo, path, branch)

        api_url = 'https://api.github.com'
        token = 'token'
        logger = Mock(spec=Logger)
        git = GitHub(api_url, token, logger)

        download_url_1 = 'https://raw.githubusercontent.com//magencio/git_to_dbfs_function/master/' +\
                'samplefiles/v0.0.1/file1.csv?token=ABACQB6ZHRWQUSV5N2EGKWK7L5YJC'
        download_url_2 = 'https://raw.githubusercontent.com//magencio/git_to_dbfs_function/master/' +\
                'samplefiles/v0.0.1/file2.csv?token=ABACQB6ZHRWQUSV5N2EGKWK7L5YJC'
        git.repos_content = Mock(return_value=[
            {'download_url': download_url_1},
            {'download_url': download_url_2}
        ])

        chunk_1 = 'chunk1'.encode()
        chunk_2 = 'chunk2'.encode()
        chunk_3 = 'chunk3'.encode()
        def download_file(download_url: str, got_chunk: Callable[[bytes], None]):
            if download_url == download_url_1:
                got_chunk(chunk_1)
            elif download_url == download_url_2:
                got_chunk(chunk_2)
                got_chunk(chunk_3)
            else:
                'chunk4'.encode()
        git.download_file = Mock(side_effect=download_file)

        dbfs_path = '/mnt/playground/magencio/data/samplefiles/v0.0.1'
        dbfs_file_path_1 = f'{dbfs_path}/file1.csv'
        dbfs_file_path_2 = f'{dbfs_path}/file2.csv'
        dbfs = Mock(spec=DBFS)

        handle_1 = 1
        handle_2 = 2
        def create(path: str, overwrite: bool):
            if path == dbfs_file_path_1:
                return handle_1
            if path == dbfs_file_path_2:
                return handle_2
            return 3
        dbfs.create = Mock(side_effect=create)

        # Act
        git.copy_folder_to_dbfs(git_path, dbfs, dbfs_path)

        # Assert
        dbfs.delete.assert_called_once_with(dbfs_path, True)
        dbfs.create.assert_has_calls([
            call(dbfs_file_path_1, True),
            call(dbfs_file_path_2, True)],
            any_order=True)
        dbfs.add_block.assert_has_calls([
            call(handle_1, chunk_1),
            call(handle_2, chunk_2), call(handle_2, chunk_3)],
            any_order=True)
        dbfs.close.assert_has_calls([
            call(handle_1),
            call(handle_2)],
            any_order=True)

    def test_copy_missing_folder_to_dbfs(self):
        """
        Tests a copy of a missing folder to a DBFS folder (which should just delete the previous
        contents of the DBFS folder).
        """
        # Arrange
        repo = 'magencio/git_to_dbfs_function'
        path = 'samplefiles/v0.0.1'
        branch = 'master'
        git_path = GitPath(repo, path, branch)

        api_url = 'https://api.github.com'
        token = 'token'
        logger = Mock(spec=Logger)
        git = GitHub(api_url, token, logger)

        git.repos_content = Mock(side_effect=GitHubException(404))

        dbfs_path = '/mnt/playground/magencio/data/samplefiles/v0.0.1'
        dbfs = Mock(spec=DBFS)

        # Act
        git.copy_folder_to_dbfs(git_path, dbfs, dbfs_path)

        # Assert
        dbfs.delete.assert_called_once_with(dbfs_path, True)
        dbfs.create.assert_not_called()
        dbfs.add_block.assert_not_called()
        dbfs.close.assert_not_called()

    def test_copy_folder_to_dbfs_with_github_error(self):
        """
        Tests a copy of all files in a folder to a DBFS folder when failing to access GitHub.
        """
        # Arrange
        repo = 'magencio/git_to_dbfs_function'
        path = 'samplefiles/v0.0.1'
        branch = 'master'
        git_path = GitPath(repo, path, branch)

        api_url = 'https://api.github.com'
        token = 'token'
        logger = Mock(spec=Logger)
        git = GitHub(api_url, token, logger)

        git.repos_content = Mock(side_effect=GitHubException(401))

        dbfs_path = '/mnt/playground/magencio/data/samplefiles/v0.0.1'
        dbfs = Mock(spec=DBFS)

        # Act & Assert
        with self.assertRaisesRegex(GitHubException, '401'):
            git.copy_folder_to_dbfs(git_path, dbfs, dbfs_path)

        dbfs.delete.assert_not_called()
        dbfs.create.assert_not_called()
        dbfs.add_block.assert_not_called()
        dbfs.close.assert_not_called()

    def test_copy_folder_to_dbfs_with_dbfs_error(self):
        """
        Tests a copy of all files in a folder to a DBFS folder when failing to access DBFS.
        """
        # Arrange
        repo = 'magencio/git_to_dbfs_function'
        path = 'samplefiles/v0.0.1'
        branch = 'master'
        git_path = GitPath(repo, path, branch)

        api_url = 'https://api.github.com'
        token = 'token'
        logger = Mock(spec=Logger)
        git = GitHub(api_url, token, logger)

        download_url_1 = 'https://raw.githubusercontent.com//magencio/git_to_dbfs_function/master/' +\
                'samplefiles/v0.0.1/file1.csv?token=ABACQB6ZHRWQUSV5N2EGKWK7L5YJC'
        download_url_2 = 'https://raw.githubusercontent.com//magencio/git_to_dbfs_function/master/' +\
                'samplefiles/v0.0.1/file2.csv?token=ABACQB6ZHRWQUSV5N2EGKWK7L5YJC'
        git.repos_content = Mock(return_value=[
            {'download_url': download_url_1},
            {'download_url': download_url_2}
        ])

        dbfs_path = '/mnt/playground/magencio/data/samplefiles/v0.0.1'
        dbfs = Mock(spec=DBFS)

        dbfs.delete = Mock(side_effect=DBFSException(401))

        # Act
        with self.assertRaisesRegex(DBFSException, '401'):
            git.copy_folder_to_dbfs(git_path, dbfs, dbfs_path)

        # Assert
        dbfs.delete.assert_called_once_with(dbfs_path, True)
        dbfs.create.assert_not_called()
        dbfs.add_block.assert_not_called()
        dbfs.close.assert_not_called()
