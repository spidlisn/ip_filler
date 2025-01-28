import os
import subprocess
import asyncio
import tqdm
import logging
import argparse
import boto3
from ipaddress import ip_address, ip_network
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text


# AUTH_ACCOUNT_MAP = {
#     "dev": "085681790652",
#     "stage": "958542800488",
#     "prod": "029959144006"
# }

DB_CONFIGS = {
    'local': 'localhost/localdevdb',
    'dev': 'devdb-mysql8-aurora-3-05-2.cluster-c1a1rfqxl7mh.eu-west-1.rds.amazonaws.com/devdb',
    'stage': '',
    'prod': ''
}


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


async def validate_region(engine, input_region):
    """
    Validates if the provided region exists in the regions table

    Args:
        engine: SQLAlchemy async engine instance
        input_region (str): Region provided through CLI args

    Returns:
        bool: True if region exists, False otherwise
    """
    async with engine.connect() as connection:
        query = text("SELECT region_name FROM region")
        result = await connection.execute(query)
        db_regions = [row[0] for row in result]

        return input_region in db_regions


async def get_db_credentials(environment, aws_region):
    """
    Fetches database credentials from AWS Secrets Manager
    """
    if environment == "local":
        return {"root": "strongpassword"}

    cmd = f'AWS_PROFILE=nataas-{environment} AWS_REGION={aws_region} summon --yaml "SECRET_RDS_CREDENTIAL: !var {environment}/api/rds" --provider summon-aws-secrets printenv SECRET_RDS_CREDENTIAL'
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    credentials = stdout.decode('utf-8').strip()

    return eval(credentials)


async def get_expanded_network_ips(expanded_network, current_network):
    """
    Returns IP addresses available in the expanded network that are not present in the current network.
    Useful when expanding from a smaller to a larger network range (e.g., /16 to /15).

    Args:
        expanded_network (str): The larger network in CIDR notation (e.g., '172.18.0.0/15')
        current_network (str): The smaller network in CIDR notation (e.g., '172.18.0.0/16')

    Returns:
        list: List of new IP addresses as strings that become available in the expanded network
    """
    expanded_range = ip_network(expanded_network)
    current_range = ip_network(current_network)

    new_available_ips = [int(ip) for ip in expanded_range.hosts() if ip not in current_range]
    return new_available_ips


async def lock_region_table(connection, region):
    """Lock table for specific region to prevent concurrent modifications"""
    try:
        lock_query = text("""
            SELECT * FROM ipaddress_inside_regional 
            WHERE region = :region 
            FOR UPDATE NOWAIT
        """)
        await connection.execute(lock_query, {"region": region})
    except Exception:
        logger.error(f"Region {region} is currently locked by another process. Please try again later.")
        raise SystemExit(1)



async def insert_ip_addresses(engine, region, ip_addresses, debug=False):
    """
    Insert new IP addresses into ipaddress_inside_regional table with region isolation
    Shows progress and optionally logs duplicate entries
    """

    total_ips = len(ip_addresses)
    inserted_count = 0
    skipped_count = 0
    progress_bar = tqdm.tqdm(total=total_ips, desc="Inserting IPs", miniters=2, ncols=100)

    async with engine.begin() as connection:
        await lock_region_table(connection, region)

        insert_query = text("""
            INSERT IGNORE INTO ipaddress_inside_regional (region, address, timestamp, inuse)
            VALUES (:region, :address, '1970-01-01 00:00:00', 0)
        """)

        for ip in ip_addresses:
            try:
                result = await connection.execute(
                    insert_query,
                    {"region": region, "address": ip}
                )
                if result.rowcount > 0:
                    inserted_count += 1
                else:
                    skipped_count += 1
            except Exception as e:
                if debug:
                    logger.error(f"Error inserting IP {ip}: {str(e)}")
                skipped_count += 1
            finally:
                progress_bar.update(1)

    progress_bar.close()
    logger.info(f"Insertion complete - Total IPs: {total_ips}, Inserted: {inserted_count}, Skipped: {skipped_count}")


def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description='Update IPs in API database')
    parser.add_argument('--env', default='dev', help='Environment (dev/stage/prod)')
    parser.add_argument('--api_region', required=True, help='API AWS region to fill IPs')
    parser.add_argument('--db_region', help='RDS instance region')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    if args.env != 'local' and not args.db_region:
        parser.error("Please provide --db_region when not running locally")

    return asyncio.run(async_main(args))

async def async_main(args):

    # Network range
    expanded_network = '172.18.0.0/15'
    current_network = '172.18.0.0/16'


    db_credentials = await get_db_credentials(args.env, args.db_region)
    username = list(db_credentials.keys())[0]
    password = db_credentials[username]
    db_host = DB_CONFIGS[args.env]

    db_url = f"mysql+aiomysql://{username}:{password}@{db_host}"
    engine = create_async_engine(db_url)

    try:
        if not await validate_region(engine, args.api_region):
            logger.error(f"Region {args.region} not found in database")
            return

        new_ips = await get_expanded_network_ips(expanded_network, current_network)
        await insert_ip_addresses(engine, args.api_region, new_ips)
    finally:
        await engine.dispose()

if __name__ == '__main__':
    main()
