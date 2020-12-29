# Git to DBFS

Http Triggered Python Azure Function that listens for WebHook Notifications from GitHub Enterprise/GitHub (code references GitHub Enterprise, but it works with GitHub, too). It detects if any file under a specific version folder got changed during a Push operation to a branch in the Git repo. If so, it uploads all the files in that version folder to DBFS in Databricks.
Path to files: {base_path}/{version}/{file}

This function is meant to be used with files in [magencio/git_to_dbfs_function](https://github.com/magencio/git_to_dbfs_function) repo, but you may change the settings to target another repo, branch and path with the same file structure.

# Development details

Code in this `git_to_dbfs_function` folder has been developed with [Visual Studio Code](https://code.visualstudio.com/) on Windows 10, using [Azure Functions extension](https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-azurefunctions) and [Python extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python) for Visual Studio Code (more details in [Useful links section](#useful-links) below).

Azure Functions and Python are multi-platform, so you should be able to continue development on either Windows, Linux or Mac. Code is configured to run with Visual Studio Code, which is multi-platform, too. But you may still use any other editor or IDE of your liking.

## Configure your development environment
See the official documentation for details: [Configure your environment](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-function-vs-code?pivots=programming-language-python#configure-your-environment).

### Visual Studio Code and Python virtual environment
To be able to run/debug the code and its unit tests, Visual Studio Code settings found in `.vscode` folder depend on the existence of a `.venv` folder containing a Python virtual environment.

While in this `git_to_dbfs_function` folder, you may create such an environment with `python -m venv ./.venv`.

### TLS/SSL error when using PIP on Python virtual environment (Windows)
If virtual environment's pip fails to install packages with an error like the following:
```
pip is configured with locations that require TLS/SSL, however the ssl module in Python is not available.
```
and you have e.g. [Anaconda](https://www.anaconda.com/) installed, add the following paths to the Windows `Path` environment variable:
```
<<path_to_your_Anaconda_installation>>\Anaconda3
<<path_to_your_Anaconda_installation>>\Anaconda3\scripts
<<path_to_your_Anaconda_installation>>\Anaconda3\library\bin
```

## Configuring GitHub Webhook for local development
First we need to expose the Http endpoint of our Azure Function using [ngrok](https://ngrok.com/): `ngrok http 7071` (or `ngrok http 7071 --region=eu` if you are in Europe).

Note the URL created by ngrok and use it as the Payload URL of your Webhook e.g. https://b3882b648fbd.ngrok.io/api/git_to_dbfs. We need the webhook to be triggered just by Push events.

Check in [Useful links](#useful-links) section below for more information on how to create a GitHub Webhook.

## Running the project locally

Make sure you have a `local.settings.json` file. See the `local.settings.sample.json` file for what this should look like.

If you are using Visual Studio Code, just run the code with `Run (Ctrl+Shift+D) > Attach to Python Functions`. It will activate the Python virtual environment and import required Python packages from `requirements.txt` file.

If you are in the console, don't forget to activate the Python virtual environment first (using `.venv/scripts/activate`) and import required Python packages (`pip install -r requirements.txt`). Then run the functions with `func start`.

## Running the project locally with Docker

Make sure you have a `.env` file. See the `.env.sample` file for what this should look like. 
Then build the docker image with `docker build -t gittodbfs .` and run it with `docker run --env-file .env -p 7071:80 gittodbfs`.

## Running the unit tests

If you are in Visual Studio Code, just click on `Test` icon to discover, run and debug the tests. If you don't see the Test icon, go to `Command Palette (Ctrl+Shift+P)` and run `Python: Discover Tests` first.

If you are in the console, activate the Python virtual environment (using `.venv/scripts/activate`), import required Python packages (`pip install -r requirements.txt`) and run `python -m unittest discover -t . --verbose`.

### View Code Coverage in Visual Studio Code

If you are using Visual Studio Code, you can install [Coverage Gutters extension](https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters).
Make sure you have the correct packages installed in the Python virtual environment (`pytest`, `coverage`, `pytest-cov`) and generate the cov.xml file running `pytest --cov=. git_to_dbfs/tests/ --cov-report xml:coverage.xml`
Open in the editor the file you want to check and then in the command palette select `Coverage Gutters: Dispaly Coverage` to immediately see which lines are covered by tests.
Please review [Coverage Gutters extension](https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters) for other settings and information.

## Looking at the logs in Azure Application Insights

The code sends its traces to Azure Application Insights. You may go to the Azure Portal and run queries in the Logs section of your Application Insights service. Examples:
```
// All traces, last one on top
traces
| order by timestamp desc

// All traces of type ERROR, last one on top
traces
| where customDimensions.level == 'ERROR'
| order by timestamp desc

// All traces for a specific request, first one on top
traces
| where operation_Id == '03859b5f784bc9619668053922338406'
| order by timestamp asc
```

# Useful links
Azure Functions
- [Work with Azure Functions Core Tools](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local)
- [Azure Functions Python developer guide](https://docs.microsoft.com/en-us/azure/azure-functions/functions-reference-python)

Azure Functions with Visual Studio Code
- [Quickstart: Create a Python function in Azure using Visual Studio Code](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-function-vs-code?pivots=programming-language-python)
- [Python testing in Visual Studio Code](https://code.visualstudio.com/docs/python/testing)

Azure Functions with Docker
- [Azure Function App and Docker on Azure made easy in 12 steps](https://dev.to/codeprototype/azure-function-app-and-docker-on-azure-made-easy-in-12-steps-3fob)

Logging with Azure Application Insights
- [opencensus-ext-azure](https://pypi.org/project/opencensus-ext-azure/)

GitHub Enterprise
- [Webhooks](https://docs.github.com/en/enterprise/2.21/user/developers/webhooks-and-events/webhooks)
- [Creating webhooks](https://docs.github.com/en/enterprise/2.21/user/developers/webhooks-and-events/creating-webhooks)
- [Webhook events and payloads: Push](https://docs.github.com/en/enterprise/2.21/user/developers/webhooks-and-events/webhook-events-and-payloads#push)
- [REST API: Repositories > Contents](https://docs.github.com/en/enterprise/2.21/user/rest/reference/repos#contents).
- [Authorizing OAuth Apps](https://docs.github.com/en/enterprise/2.21/user/developers/apps/authorizing-oauth-apps) and [Other authentication methods](https://docs.github.com/en/enterprise/2.21/user/rest/overview/other-authentication-methods#via-oauth-and-personal-access-tokens)

Databricks:
- [DBFS API](https://docs.databricks.com/dev-tools/api/latest/dbfs.html)
- [Authentication using Databricks personal access tokens](https://docs.databricks.com/dev-tools/api/latest/authentication.html)