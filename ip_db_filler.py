import os
import json
import logging
import argparse
import boto3
import subprocess
import tempfile
from ipaddress import ip_network
from sqlalchemy import create_engine, text

DB_CONFIGS = {
    'local': 'localhost/localdevdb',
    'dev': 'devdb-mysql8-aurora-3-05-2.cluster-c1a1rfqxl7mh.eu-west-1.rds.amazonaws.com/devdb',
    'stage': 'stagedb-mysql8-aurora-3-05-2.cluster-c0cmryw6wtox.us-east-1.rds.amazonaws.com/stagedb',
    'prod': 'proddb-mysql8-aurora-3-05-2.cluster-c20yinhaflta.us-east-1.rds.amazonaws.com/proddb'
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def validate_region(engine, input_region):
    """Validates if the provided region exists in the regions table"""
    with engine.connect() as connection:
        query = text("SELECT region_name FROM region")
        result = connection.execute(query)
        db_regions = [row[0] for row in result]
        return input_region in db_regions


def get_db_credentials(environment, aws_region):
    """Fetches database credentials from AWS Secrets Manager"""
    if environment == "local":
        return {"root": "strongpassword"}

    session = boto3.session.Session(profile_name=f'nataas-{environment}', region_name=aws_region)
    client = session.client(
        service_name='secretsmanager',
        region_name=aws_region
    )

    secret_name = f"{environment}/api/rds"
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except client.exceptions.ResourceNotFoundException:
        logger.error(f"Secret not found: {secret_name}")
        raise SystemExit(1)


def get_expanded_network_ips(expanded_network, current_network):
    """Returns new IP addresses available in the expanded network"""
    expanded_range = ip_network(expanded_network)
    current_range = ip_network(current_network)
    return [int(ip) for ip in expanded_range.hosts() if ip not in current_range]


def generate_dump_file(region, ip_addresses):
    """Generate MySQL dump file with new IPs"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql') as f:
        f.write("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n")
        f.write("/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;\n")
        f.write("/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;\n")
        f.write("/*!40101 SET NAMES utf8 */;\n")
        f.write("SET foreign_key_checks=0;\n")
        f.write("SET unique_checks=0;\n")
        f.write("SET autocommit=0;\n")

        # Write INSERT IGNORE statement
        f.write("INSERT IGNORE INTO ipaddress_inside_regional (region, address, timestamp, inuse) VALUES\n")

        # Generate values
        values = []
        for ip in ip_addresses:
            values.append(f"('{region}', {ip}, '1970-01-01 00:00:00 UTC', 0)")

        f.write(",\n".join(values) + ";\n")

        f.write("SET foreign_key_checks=1;\n")
        f.write("SET unique_checks=1;\n")
        f.write("SET autocommit=1;\n")
        f.write("COMMIT;\n")
        return f.name


def load_dump(engine, dump_file, db_credentials, env):
    """Load dump file into database using mysql client"""
    host = DB_CONFIGS[env].split('/')[0]
    database = DB_CONFIGS[env].split('/')[1]
    username = list(db_credentials.keys())[0]
    password = db_credentials[username]

    cmd = [
        'mysql',
        f'-h{host}',
        f'-u{username}',
        f'-p{password}',
        f'--protocol=TCP',
        f'--port=3306',
        database
    ]

    with open(dump_file, 'r') as f:
        subprocess.run(cmd, stdin=f, check=True)


def main():
    parser = argparse.ArgumentParser(description='Bulk load IPs into API database using MySQL dump')
    parser.add_argument('--env', default='dev', help='Environment (dev/stage/prod)')
    parser.add_argument('--api_region', required=True, help='API AWS region to fill IPs')
    parser.add_argument('--db_region', help='RDS instance region')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()
    if args.env != 'local' and not args.db_region:
        parser.error("Please provide --db_region when not running locally")

    expanded_network = '172.18.0.0/15'
    current_network = '172.18.0.0/16'

    db_credentials = get_db_credentials(args.env, args.db_region)
    db_host = DB_CONFIGS[args.env]
    username = list(db_credentials.keys())[0]
    password = db_credentials[username]

    db_url = f"mysql+pymysql://{username}:{password}@{db_host}"
    engine = create_engine(db_url)

    try:
        if not validate_region(engine, args.api_region):
            logger.error(f"Region {args.api_region} not found in database")
            return

        logger.info("Calculating IP addresses...")
        new_ips = get_expanded_network_ips(expanded_network, current_network)

        logger.info(f"Generating dump file for {len(new_ips)} addresses...")
        dump_file = generate_dump_file(args.api_region, new_ips)

        logger.info("Loading dump into database...")
        load_dump(engine, dump_file, db_credentials, args.env)
        logger.info("Import completed successfully")

    finally:
        if 'dump_file' in locals():
            os.unlink(dump_file)


if __name__ == '__main__':
    main()
