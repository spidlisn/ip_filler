# ip_filler
Fill the DB with the missed IP's

## Overview
A Python tool that efficiently identifies and fills missing IP addresses in your database. Perfect for maintaining complete IP address sequences and ensuring data continuity.

## Installation

### Directly from a repository
```bash
pipx install git+ssh://git@github.com/spidlisn/ip_filler.git
```
### Option 2: Local Development
1. Clone the repository:
```bash
git clone https://github.com/spidlisn/ip_filler.git
```
2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```
3. Install setup.py:
```bash
pip install -e .
```
**Note** If you have any issues, you can install requirements.txt:
```bash
pip install -r requirements.txt
```
## Usage
```bash
usage: ip_db_filler [-h] [--env ENV] --api_region API_REGION [--db_region DB_REGION] [--debug]

Update IPs in API database

options:
  -h, --help            show this help message and exit
  --env ENV             Environment (local/dev/stage/prod)
  --api_region API_REGION
                        API AWS region to fill IPs (Region for usage during the IP insert operations)
  --db_region DB_REGION
                        RDS instance region (Region for the Secret Manager to get the DB creds)
  --debug               Enable debug logging

```
**Note**: Stage and Prod envs currently unavailable.

## Example
***If you are using not local envoriment, you need to login first, to enable RDS access. For example for the DEV 
we have to use `aws-nataas-dev` command for login. This step might be fixed later*** 
```bash
ip_db_filler --env local --api_region dev_aws_us-west-2_3 --debug
```
**Note** You don't need to specify the RDS db_region for the local environment.

```bash
ip_db_filler --env dev --api_region dev_aws_us-west-2_3 --db_region eu-west-1 --debug
```
