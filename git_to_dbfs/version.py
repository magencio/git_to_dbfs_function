"""
Helper methods related to version folders.
"""
import copy
from logging import Logger
import re
from typing import List, Set

from .services import GitHub, GitPath, DBFS

def get_versions(base_path: str, files: List[str]) -> set:
    """
    Gets the versions of all files with path {base_path}/{version}/{file}
    """
    versions = set()
    for file in files:
        path_parts = re.sub(f'^{base_path}/', '', file, 1).split('/')
        if len(path_parts) == 2:
            versions.add(path_parts[0])
    return versions

def copy_versions_to_dbfs(
    versions: Set[str],
    git: GitHub, git_base_path: GitPath,
    dbfs: DBFS, dbfs_base_path: str,
    logger: Logger):
    """
    Copy all files in version folders from GitHub to Databricks
    """
    for version in versions:
        logger.info('Version "%s" has been modified', version)
        git_path = copy.deepcopy(git_base_path)
        git_path.path = f'{git_base_path.path}/{version}'
        dbfs_path = f'{dbfs_base_path}/{version}'
        git.copy_folder_to_dbfs(git_path, dbfs, dbfs_path)
