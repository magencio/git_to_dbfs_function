"""
Http Triggered Azure Function that listens for WebHook Notifications from GitHub.
It detects if any file under a specific version folder got changed during a Push
operation to a branch in the Git repo. If so, it uploads all the files in that version
folder to DBFS in Databricks.

Path to files: {base_path}/{version}/{file}
"""

import copy
import json
import logging
import os
import requests

import azure.functions as func
from opencensus.ext.azure.log_exporter import AzureLogHandler
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.trace import config_integration
from opencensus.trace.samplers import AlwaysOnSampler
from opencensus.trace.samplers import ProbabilitySampler
from opencensus.trace.tracer import Tracer

from .version import *
from .services import *

def __get_logger() -> Logger:
    # Create a logger with Azure Application Insights
    config_integration.trace_integrations(['logging'])
    logger = logging.getLogger(__name__)
    handler = AzureLogHandler(connection_string=os.getenv('ApplicationInsights'))
    handler.setFormatter(logging.Formatter('%(traceId)s %(message)s'))
    logger.addHandler(handler)
    return logger

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Entry point for this Azure Function.
    """
    logger = __get_logger()
    logger.info('Python HTTP trigger function processed a request.')

    # Verify request comes from GitHub Webhook
    if not validate_payload(req, os.getenv('WebhookSecret')):
        logger.error('Forbidden request')
        return func.HttpResponse('Forbidden.', status_code=403)

    # Process "Git push to a repository" notifications
    try:
        notification = GitPushNotification(req.get_json())
    except AttributeError:
        logger.warning('Ignoring notification: Invalid type')
        return func.HttpResponse('Ignoring notification: Invalid type', status_code=200)

    # Get the version folders that got modified under Git base path
    git_base_path = GitPath(os.getenv('GitRepo'), os.getenv('GitBasePath'), os.getenv('GitBranch'))
    logger.info('Checking for modified version folders '
        '[GitHub Repo "%s", Branch "%s", Base Path "%s"]',
        git_base_path.repo, git_base_path.branch, git_base_path.path)

    modified_files = notification.get_modified_files(git_base_path)

    versions = get_versions(git_base_path.path, modified_files)
    if len(versions) == 0:
        logger.warning('Ignoring notification: No version folders modified')
        return func.HttpResponse('Ignoring notification: No version folders modified',
            status_code=200)

    # Copy all files in modified version folders from GitHub to Databricks
    try:
        git = GitHub(os.getenv('GitApi'), os.getenv('GitToken'), logger)
        dbfs = DBFS(os.getenv('DatabricksHost'), os.getenv('DatabricksToken'))
        dbfs_base_path = os.getenv('DatabricksDbfsBasePath')
        copy_versions_to_dbfs(versions, git, git_base_path, dbfs, dbfs_base_path, logger)
    except GitHubException as ex:
        logger.exception('Failed to access GitHub files', exc_info=ex)
        return func.HttpResponse('Failed to access GitHub files.', status_code=500)
    except DBFSException as ex:
        logger.exception('Failed to access DBFS', exc_info=ex)
        return func.HttpResponse('Failed to access DBFS.', status_code=500)

    return func.HttpResponse('Notification processed successfully.', status_code=200)
