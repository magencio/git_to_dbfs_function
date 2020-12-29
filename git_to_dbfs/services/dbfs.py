"""
DBFS API Wrapper and related classes.
More info: https://docs.databricks.com/dev-tools/api/latest/dbfs.html
"""

import base64
import requests

class DBFSException(Exception):
    """
    Exception when accessing DBFS API.
    """

class DBFS:
    """
    Class to access DBFS in Databricks
    """

    def __init__(self, host: str, token: str):
        self.__host = host
        self.__headers = {'Authorization': f'Bearer {token}'}

    def list(self, path: str) -> dict:
        """
        List the contents of a directory, or details of the file.
        If the file or directory does not exist, this call throws an exception.
        Example of reply:
        {
            "files": [
                {
                    "path": "/a.cpp",
                    "is_dir": false,
                    "file_size": 261
                },
                {
                    "path": "/databricks-results",
                    "is_dir": true,
                    "file_size": 0
                }
            ]
        }

        More info:
            https://docs.databricks.com/dev-tools/api/latest/dbfs.html#list
            https://docs.databricks.com/dev-tools/api/latest/dbfs.html#dbfsfileinfo
        """
        return self.__get('list', params={'path': path}).json()

    def mkdirs(self, path: str):
        """
        Create the given directory and necessary parent directories if they do not exist.
        If there exists a file (not a directory) at any prefix of the input path,
        this call throws an exception.

        More info: https://docs.databricks.com/dev-tools/api/latest/dbfs.html#mkdirs
        """
        self.__post('mkdirs', data={'path': path})

    def create(self, path: str, overwrite: bool) -> int:
        """
        Open a stream to write to a file and returns a handle to this stream.
        There is a 10 minute idle timeout on this handle.
        If a file or directory already exists on the given path and overwrite is set to false,
        this call throws an exception.

        More info: https://docs.databricks.com/dev-tools/api/latest/dbfs.html#create
        """
        response = self.__post('create', data={'path': path, 'overwrite': overwrite})
        return response.json()['handle']

    def add_block(self, handle: int, data: bytes):
        """
        Append a block of data to the stream specified by the input handle.
        If the handle does not exist, this call will throw an exception.
        If the block of data exceeds 1 MB, this call will throw an exception.

        More info: https://docs.databricks.com/dev-tools/api/latest/dbfs.html#add-block
        """
        base64_data = str(base64.b64encode(data), 'utf-8')
        self.__post('add-block', data={'handle': handle, 'data': base64_data})

    def close(self, handle: int):
        """
        Close the stream specified by the input handle.
        If the handle does not exist, this call throws an exception.

        More info: https://docs.databricks.com/dev-tools/api/latest/dbfs.html#close
        """
        self.__post('close', data={'handle': handle})

    def delete(self, path: str, recursive: bool):
        """
        Delete the file or directory (optionally recursively delete all files in the directory).
        This call throws an exception if the path is a non-empty directory and recursive is set to
        false or on other similar errors.

        TODO: Deal with this: When you delete a large number of files, the delete operation is done
        in increments. The call returns a response after approximately 45s with an error message
        asking you to re-invoke the delete operation until the directory structure is fully
        deleted.
        For example:
        {
            "error_code":"PARTIAL_DELETE","message":"The requested operation has deleted 324
        files. There are more files remaining. You must make another request to delete more."
        }

        More info: https://docs.databricks.com/dev-tools/api/latest/dbfs.html#delete
        """
        self.__post('delete', data={'path': path, 'recursive': recursive})

    def __get(self, api: str, params: dict) -> requests.Response:
        response = requests.get(f'{self.__host}/api/2.0/dbfs/{api}', headers=self.__headers,
            params=params)
        if not response:
            raise DBFSException(response.status_code)
        return response

    def __post(self, api: str, data: dict) -> requests.Response:
        response = requests.post(f'{self.__host}/api/2.0/dbfs/{api}', headers=self.__headers,
            json=data)
        if not response:
            raise DBFSException(response.status_code)
        return response
