import os
import subprocess
import json
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

    session = boto3.session.Session(profile_name=f'nataas-{environment}', region_name=aws_region)
    client = session.client(
        service_name='secretsmanager',
        region_name=aws_region
    )

    # Match the exact path format from summon command
    secret_name = f"{environment}/api/rds"
    try:
        response = client.get_secret_value(SecretId=secret_name)
        credentials = json.loads(response['SecretString'])
        return credentials
    except client.exceptions.ResourceNotFoundException:
        logger.error(f"Secret not found: {secret_name}")
        raise SystemExit(1)


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


async def process_batch(connection, batch_id, region, batch_ips, progress_bar):
    """Process single batch of IPs with its own progress bar"""
    inserted_count = 0
    skipped_count = 0
    chunk_size = 100  # Smaller chunks for more frequent updates

    insert_query = text("""
        INSERT IGNORE INTO ipaddress_inside_regional (region, address, timestamp, inuse)
        VALUES (:region, :address, '1970-01-01 00:00:00', 0)
    """)

    for i in range(0, len(batch_ips), chunk_size):
        chunk = batch_ips[i:i + chunk_size]
        params = [{"region": region, "address": ip} for ip in chunk]
        result = await connection.execute(insert_query, params)
        inserted_count += result.rowcount
        skipped_count += len(chunk) - result.rowcount
        progress_bar.update(len(chunk))
        progress_bar.set_postfix({'inserted': inserted_count, 'skipped': skipped_count}, refresh=True)
        await asyncio.sleep(0.1)  # Small delay to allow UI updates

    return inserted_count, skipped_count


async def insert_ip_addresses(engine, region, ip_addresses, debug=False):
    total_ips = len(ip_addresses)
    batch_size = total_ips // 3

    progress_bars = [
        tqdm.tqdm(
            total=batch_size,
            desc=f"Batch {i + 1}/3",
            position=i,
            leave=True,
            miniters=1,
            mininterval=0.1
        ) for i in range(3)
    ]

    async with engine.begin() as connection:
        batches = [
            ip_addresses[i:i + batch_size]
            for i in range(0, total_ips, batch_size)
        ]

        tasks = [
            process_batch(connection, i, region, batch, progress_bars[i])
            for i, batch in enumerate(batches[:3])
        ]

        results = await asyncio.gather(*tasks)

        total_inserted = sum(r[0] for r in results)
        total_skipped = sum(r[1] for r in results)

    for bar in progress_bars:
        bar.close()

    logger.info(f"Concurrent insertion complete - Total IPs: {total_ips}, "
                f"Inserted: {total_inserted}, Skipped: {total_skipped}")


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
        # logger.info(await validate_region(engine, args.api_region))
        if not await validate_region(engine, args.api_region):
            logger.error(f"Region {args.region} not found in database")
            return

        new_ips = await get_expanded_network_ips(expanded_network, current_network)
        await insert_ip_addresses(engine, args.api_region, new_ips)
    finally:
        await engine.dispose()

if __name__ == '__main__':
    main()
