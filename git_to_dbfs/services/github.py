"""
GitHub Enterprise API Wrapper, helper methods and related classes.
More info: https://docs.github.com/en/enterprise/2.21/user/rest
"""

from hashlib import sha1
from hmac import HMAC, compare_digest
from logging import Logger
import re
from typing import Callable
import requests

import azure.functions as func

from . import DBFS

def validate_payload(req: func.HttpRequest, secret: str) -> bool:
    """
    Validate payload from GitHub Webhook.
    """
    x_hub_signature = req.headers.get('X-Hub-Signature')
    if not x_hub_signature or not re.match('^sha1=.+', x_hub_signature):
        return False

    signature = re.sub('^sha1=', '', x_hub_signature, 1)
    encoded_secret = secret.encode()
    expected_signature = HMAC(key=encoded_secret, msg=req.get_body(), digestmod=sha1)\
        .hexdigest()
    return compare_digest(signature, expected_signature)

class GitPath:
    """
    Git Path.
    """
    def __init__(self, repo: str, path: str, branch: str):
        self.repo = repo
        self.path = path
        self.branch = branch
        self.ref = f'refs/heads/{branch}'

class GitHubException(Exception):
    """
    Exception when accessing GitHub Enterprise API.
    """

class GitHub:
    """
    Class to access GitHub Enterprise.
    """

    def __init__(self, api_base_url: str, token: str, logger: Logger):
        self.__api_base_url = api_base_url
        self.__headers = {'Authorization': f'Bearer {token}'}
        self.__logger = logger

    def repos_content(self, path: GitPath) -> dict:
        """
        Gets the contents of a file or directory in a repository.

        More info:
        https://docs.github.com/en/enterprise/2.21/user/rest/reference/repos#get-repository-content
        """
        return self.__get(f'repos/{path.repo}/contents/{path.path}', params={'ref': path.branch})\
            .json()

    def download_file(self, download_url: str, got_chunk: Callable[[bytes], None]):
        """
        Downloads a file in chunks.
        """
        with requests.get(download_url, headers=self.__headers, stream=True) as response:
            if not response:
                raise GitHubException(response.status_code)

            for chunk in response.iter_content(chunk_size=8192):
                got_chunk(chunk)

    def copy_folder_to_dbfs(self, git_path: GitPath, dbfs: DBFS, dbfs_path: str):
        """
        Copy all files in a folder to a DBFS folder.
        All previous contents of DBFS folder will be deleted.
        """
        contents = None
        try:
            contents = self.repos_content(git_path)

            self.__logger.info('Deleting all files in DBFS folder "%s"', dbfs_path)
            dbfs.delete(dbfs_path, True)

            download_urls = [x['download_url'] for x in contents]
            for download_url in download_urls:
                self.__copy_file_to_dbfs(download_url, dbfs, dbfs_path)

        except GitHubException as ex:
            if str(ex) == '404' and not contents:
                # {base_path}/{version} is missing after the changes
                self.__logger.info('Deleting all files in DBFS folder "%s"', dbfs_path)
                dbfs.delete(dbfs_path, True)
            else:
                raise

    def __copy_file_to_dbfs(self, download_url: str, dbfs: DBFS, dbfs_path: str):
        file_name = download_url.split('?')[0].split('/')[-1]
        dbfs_file_path = f'{dbfs_path}/{file_name}'

        self.__logger.info('Copying GitHub file "%s" to DBFS "%s"', download_url, dbfs_file_path)

        handle = dbfs.create(dbfs_file_path, True)
        self.download_file(download_url, lambda c, h=handle: dbfs.add_block(h, c))
        dbfs.close(handle)

    def __get(self, api: str, params: dict) -> requests.Response:
        response = requests.get(f'{self.__api_base_url}/{api}', headers=self.__headers,
            params=params)
        if not response:
            raise GitHubException(response.status_code)
        return response

class GitPushNotification:
    """
    Class that represents a "Git push to a repository" notification from a GitHub Webhook.
    """

    def __init__(self, notification: dict):
        self.ref = notification.get('ref')
        repo = notification.get('repository')
        self.repo = repo.get('full_name') if repo else None
        self.commits = notification.get('commits')

        if not self.ref or not self.repo or not self.commits:
            raise AttributeError

    def get_modified_files(self, git_path: GitPath):
        """
        Get all modified files under a specific repo/branch/path.
        """
        if self.repo != git_path.repo or self.ref != git_path.ref:
            return []

        return [file
            for commit in self.commits
            for file in commit['added'] + commit['removed'] + commit['modified']
            if file.startswith(git_path.path)]
