FROM python:3.8-slim AS compile

# Install build tools
RUN apt-get update
RUN apt-get install -y --no-install-recommends build-essential gcc

# Create Python virtual environment
RUN python -m venv /.venv
ENV PATH="/.venv/bin:$PATH"

# Install required Python packages in virtual environment
COPY requirements.txt /
RUN python -m pip install --upgrade pip
RUN pip install -r /requirements.txt

# To enable ssh & remote debugging on app service change the base image to the one below
# FROM mcr.microsoft.com/azure-functions/python:3.0-python3.8-appservice AS build
FROM mcr.microsoft.com/azure-functions/python:3.0-python3.8 AS build

# Get virtual environment from compile stage
COPY --from=compile /.venv /.venv
ENV PATH="/.venv/bin:$PATH"

# Copy and setup Azure Function
COPY . /home/site/wwwroot
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true
