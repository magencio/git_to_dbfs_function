"""
Tests for dbfs.py
"""

import unittest
from unittest.mock import patch

from ...services import DBFS, DBFSException

class TestDBFS(unittest.TestCase):
    """
    Tests for DBFS class.
    """

    @patch('requests.get')
    def test_list(self, mock_get):
        """
        Tests listing the contents of a directory or details of a file.
        """
        # Arrange
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            'files': [
                {
                    'path': '/file1.csv',
                    'is_dir': False,
                    'file_size': 261
                },
                {
                    'path': '/file2.csv',
                    'is_dir': False,
                    'file_size': 542
                }
            ]
        }

        host = 'https://somehost.azuredatabricks.net'
        token = 'token'
        dbfs = DBFS(host, token)

        path = '/mnt/playground/magencio/data/samplefiles/v0.0.1'

        # Act
        result = dbfs.list(path)

        # Assert
        self.assertDictEqual(mock_get.return_value.json.return_value, result)

        mock_get.assert_called_once_with(f'{host}/api/2.0/dbfs/list',
            headers={'Authorization': f'Bearer {token}'}, params={'path': path})

    @patch('requests.get')
    def test_list_with_dbfs_error(self, mock_get):
        """
        Tests trying to list the contents of a directory or details of a file but getting a DBFS
        error back.
        """
        # Arrange
        mock_get.return_value.status_code = 401
        mock_get.return_value.__bool__.return_value = False

        host = 'https://somehost.azuredatabricks.net'
        token = 'token'
        dbfs = DBFS(host, token)

        path = '/mnt/playground/magencio/data/samplefiles/v0.0.1'

        # Act & Assert
        with self.assertRaisesRegex(DBFSException, '401'):
            dbfs.list(path)

        mock_get.assert_called_once_with(f'{host}/api/2.0/dbfs/list',
            headers={'Authorization': f'Bearer {token}'}, params={'path': path})

    @patch('requests.post')
    def test_mkdirs(self, mock_post):
        """
        Tests the creation of a directory.
        """
        # Arrange
        mock_post.return_value.status_code = 200

        host = 'https://somehost.azuredatabricks.net'
        token = 'token'
        dbfs = DBFS(host, token)

        path = '/mnt/playground/magencio/data/samplefiles/v0.0.1'

        # Act
        dbfs.mkdirs(path)

        # Assert
        mock_post.assert_called_once_with(f'{host}/api/2.0/dbfs/mkdirs',
            headers={'Authorization': f'Bearer {token}'}, json={'path': path})

    @patch('requests.post')
    def test_mkdirs_with_dbfs_error(self, mock_post):
        """
        Tests trying the creation of a directory but getting a DBFS error back.
        """
        # Arrange
        mock_post.return_value.status_code = 401
        mock_post.return_value.__bool__.return_value = False

        host = 'https://somehost.azuredatabricks.net'
        token = 'token'
        dbfs = DBFS(host, token)

        path = '/mnt/playground/magencio/data/samplefiles/v0.0.1'

        # Act & Assert
        with self.assertRaisesRegex(DBFSException, '401'):
            dbfs.mkdirs(path)

        mock_post.assert_called_once_with(f'{host}/api/2.0/dbfs/mkdirs',
            headers={'Authorization': f'Bearer {token}'}, json={'path': path})

    @patch('requests.post')
    def test_create(self, mock_post):
        """
        Tests the creation of a file stream.
        """
        # Arrange
        mock_post.return_value.status_code = 200
        handle = 1234234
        mock_post.return_value.json.return_value = {'handle': handle}

        host = 'https://somehost.azuredatabricks.net'
        token = 'token'
        dbfs = DBFS(host, token)

        path = '/mnt/playground/magencio/data/samplefiles/v0.0.1/file1.csv'
        overwrite = True

        # Act
        result = dbfs.create(path, overwrite=overwrite)

        # Assert
        self.assertEqual(handle, result)

        mock_post.assert_called_once_with(f'{host}/api/2.0/dbfs/create',
            headers={'Authorization': f'Bearer {token}'},
            json={'path': path, 'overwrite': overwrite})

    @patch('requests.post')
    def test_create_with_dbfs_error(self, mock_post):
        """
        Tests trying the creation of a file stream but getting a DBFS error back.
        """
        # Arrange
        mock_post.return_value.status_code = 401
        mock_post.return_value.__bool__.return_value = False

        host = 'https://somehost.azuredatabricks.net'
        token = 'token'
        dbfs = DBFS(host, token)

        path = '/mnt/playground/magencio/data/samplefiles/v0.0.1/file1.csv'
        overwrite = True

        # Act & Assert
        with self.assertRaisesRegex(DBFSException, '401'):
            dbfs.create(path, overwrite=overwrite)

        mock_post.assert_called_once_with(f'{host}/api/2.0/dbfs/create',
            headers={'Authorization': f'Bearer {token}'},
            json={'path': path, 'overwrite': overwrite})

    @patch('requests.post')
    def test_add_block(self, mock_post):
        """
        Tests the appending of a block of data to a stream.
        """
        # Arrange
        mock_post.return_value.status_code = 200

        host = 'https://somehost.azuredatabricks.net'
        token = 'token'
        dbfs = DBFS(host, token)

        handle = 1234234
        data = 'somedata'.encode()
        base64_data = 'c29tZWRhdGE='

        # Act
        dbfs.add_block(handle, data)

        # Assert
        mock_post.assert_called_once_with(f'{host}/api/2.0/dbfs/add-block',
            headers={'Authorization': f'Bearer {token}'},
            json={'handle': handle, 'data': base64_data})

    @patch('requests.post')
    def test_add_block_with_dbfs_error(self, mock_post):
        """
        Tests trying to append a block of data to a stream but but getting a DBFS error back.
        """
        # Arrange
        mock_post.return_value.status_code = 401
        mock_post.return_value.__bool__.return_value = False

        host = 'https://somehost.azuredatabricks.net'
        token = 'token'
        dbfs = DBFS(host, token)

        handle = 1234234
        data = 'somedata'.encode()
        base64_data = 'c29tZWRhdGE='

        # Act & Assert
        with self.assertRaisesRegex(DBFSException, '401'):
            dbfs.add_block(handle, data)

        mock_post.assert_called_once_with(f'{host}/api/2.0/dbfs/add-block',
            headers={'Authorization': f'Bearer {token}'},
            json={'handle': handle, 'data': base64_data})

    @patch('requests.post')
    def test_close(self, mock_post):
        """
        Tests the closing of a file stream.
        """
        # Arrange
        mock_post.return_value.status_code = 200

        host = 'https://somehost.azuredatabricks.net'
        token = 'token'
        dbfs = DBFS(host, token)

        handle = 1234234

        # Act
        dbfs.close(handle)

        # Assert
        mock_post.assert_called_once_with(f'{host}/api/2.0/dbfs/close',
            headers={'Authorization': f'Bearer {token}'}, json={'handle': handle})

    @patch('requests.post')
    def test_close_with_dbfs_error(self, mock_post):
        """
        Tests trying to close a file stream but getting a DBFS error back.
        """
        # Arrange
        mock_post.return_value.status_code = 401
        mock_post.return_value.__bool__.return_value = False

        host = 'https://somehost.azuredatabricks.net'
        token = 'token'
        dbfs = DBFS(host, token)

        handle = 1234234

        # Act & Assert
        with self.assertRaisesRegex(DBFSException, '401'):
            dbfs.close(handle)

        mock_post.assert_called_once_with(f'{host}/api/2.0/dbfs/close',
            headers={'Authorization': f'Bearer {token}'}, json={'handle': handle})

    @patch('requests.post')
    def test_delete(self, mock_post):
        """
        Tests the deletion of a file or folder.
        """
        # Arrange
        mock_post.return_value.status_code = 200

        host = 'https://somehost.azuredatabricks.net'
        token = 'token'
        dbfs = DBFS(host, token)

        path = '/mnt/playground/magencio/data/samplefiles/v0.0.1/file1.csv'
        recursive = False

        # Act
        dbfs.delete(path ,recursive=recursive)

        # Assert
        mock_post.assert_called_once_with(f'{host}/api/2.0/dbfs/delete',
            headers={'Authorization': f'Bearer {token}'},
            json={'path': path, 'recursive': recursive})

    @patch('requests.post')
    def test_delete_with_dbfs_error(self, mock_post):
        """
        Tests trying to delete a file or folder but getting a DBFS error back.
        """
        # Arrange
        mock_post.return_value.status_code = 401
        mock_post.return_value.__bool__.return_value = False

        host = 'https://somehost.azuredatabricks.net'
        token = 'token'
        dbfs = DBFS(host, token)

        path = '/mnt/playground/magencio/data/samplefiles/v0.0.1/file1.csv'
        recursive = False

        # Act & Assert
        with self.assertRaisesRegex(DBFSException, '401'):
            dbfs.delete(path ,recursive=recursive)

        mock_post.assert_called_once_with(f'{host}/api/2.0/dbfs/delete',
            headers={'Authorization': f'Bearer {token}'},
            json={'path': path, 'recursive': recursive})
