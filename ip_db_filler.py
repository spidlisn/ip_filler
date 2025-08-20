import os
import json
import logging
import argparse
import boto3
import subprocess
import tempfile
import datetime
from ipaddress import ip_network, ip_address
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
    """Returns new IP addresses available in the expanded network including broadcast IP"""
    expanded_range = ip_network(expanded_network)
    current_range = ip_network(current_network)

    # Get all host IPs plus broadcast IP
    all_ips = list(expanded_range.hosts()) + [expanded_range.broadcast_address]

    return [int(ip) for ip in all_ips if ip not in current_range]


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
            values.append(f"('{region}', {ip}, '1970-01-01 00:00:00', 0)")

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


def display_operation_preview(expanded_network, current_network, region, env, new_ips):
    """Display a preview of the operation that will be performed"""
    expanded_range = ip_network(expanded_network)
    current_range = ip_network(current_network)

    print("\n" + "="*60)
    print("OPERATION PREVIEW")
    print("="*60)
    print(f"Environment: {env}")
    print(f"Target Region: {region}")
    print(f"Database: {DB_CONFIGS[env]}")
    print()
    print("Network Expansion:")
    print(f"  Current Network: {current_network}")
    print(f"  Expanded Network: {expanded_network}")
    print(f"  Current IPs: {current_range.num_addresses:,} addresses")
    print(f"  Expanded IPs: {expanded_range.num_addresses:,} addresses")
    print(f"  New IPs to add: {len(new_ips):,} addresses")
    print(f"  Total IPs after expansion: {current_range.num_addresses + len(new_ips):,} addresses")
    print()

    if len(new_ips) > 0:
        print("IP Address Range Summary:")
        first_ip = ip_address(min(new_ips))
        last_ip = ip_address(max(new_ips))
        print(f"  First new IP: {first_ip}")
        print(f"  Last new IP: {last_ip}")
        print(f"  Broadcast IP included: {expanded_range.broadcast_address in [ip_address(ip) for ip in new_ips]}")

        # Show first few and last few IPs for verification
        if len(new_ips) <= 10:
            print(f"  All IPs: {', '.join([str(ip_address(ip)) for ip in sorted(new_ips)])}")
        else:
            sorted_ips = sorted(new_ips)
            first_5 = [str(ip_address(ip)) for ip in sorted_ips[:5]]
            last_5 = [str(ip_address(ip)) for ip in sorted_ips[-5:]]
            print(f"  First 5 IPs: {', '.join(first_5)}")
            print(f"  Last 5 IPs: {', '.join(last_5)}")

    print()
    print("Database Operation:")
    print(f"  Table: ipaddress_inside_regional")
    print(f"  Operation: INSERT IGNORE")
    print(f"  Records to insert: {len(new_ips):,}")
    print("="*60)


def get_user_confirmation():
    """Get user confirmation before proceeding with the operation"""
    while True:
        response = input("\nDo you want to proceed with this operation? (yes/no/show-sample): ").strip().lower()

        if response in ['yes', 'y']:
            return True
        elif response in ['no', 'n']:
            return False
        elif response in ['show-sample', 'sample', 's']:
            return 'show-sample'
        else:
            print("Please enter 'yes', 'no', or 'show-sample'")


def show_sample_sql(region, ip_addresses, sample_size=10):
    """Show a sample of the SQL that will be executed"""
    print(f"\nSample SQL (showing {min(sample_size, len(ip_addresses))} of {len(ip_addresses)} records):")
    print("-" * 60)

    sample_ips = sorted(ip_addresses)[:sample_size]

    print("INSERT IGNORE INTO ipaddress_inside_regional (region, address, timestamp, inuse) VALUES")
    for i, ip in enumerate(sample_ips):
        comma = "," if i < len(sample_ips) - 1 else ";"
        print(f"('{region}', {ip}, '1970-01-01 00:00:00', 0){comma}")

    if len(ip_addresses) > sample_size:
        print(f"... and {len(ip_addresses) - sample_size} more records")

    print("-" * 60)


def create_backup(engine, region, backup_dir=None):
    """Create a backup of existing IP addresses for the region"""
    if backup_dir is None:
        backup_dir = "ip_backups"
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)

    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = os.path.join(backup_dir, f'ip_backup_{region}_{timestamp}.sql')

    logger.info(f"Creating backup for region {region}...")

    with engine.connect() as connection:
        # Get current IP addresses for the region
        query = text("""
            SELECT region, address, timestamp, inuse 
            FROM ipaddress_inside_regional 
            WHERE region = :region
            ORDER BY address
        """)
        result = connection.execute(query, {'region': region})
        existing_ips = result.fetchall()

    if not existing_ips:
        logger.warning(f"No existing IP addresses found for region {region}")
        return None

    # Create backup SQL file
    with open(backup_file, 'w') as f:
        f.write("-- IP Address Backup\n")
        f.write(f"-- Region: {region}\n")
        f.write(f"-- Backup Date: {datetime.datetime.now().isoformat()}\n")
        f.write(f"-- Total Records: {len(existing_ips)}\n")
        f.write("\n")

        f.write("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n")
        f.write("/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;\n")
        f.write("/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;\n")
        f.write("/*!40101 SET NAMES utf8 */;\n")
        f.write("SET foreign_key_checks=0;\n")
        f.write("SET unique_checks=0;\n")
        f.write("SET autocommit=0;\n\n")

        # First delete all existing records for this region
        f.write(f"DELETE FROM ipaddress_inside_regional WHERE region = '{region}';\n\n")

        # Then restore the backed up data
        f.write("INSERT INTO ipaddress_inside_regional (region, address, timestamp, inuse) VALUES\n")

        values = []
        for row in existing_ips:
            region_val, address, timestamp, inuse = row
            timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S') if timestamp else '1970-01-01 00:00:00'
            values.append(f"('{region_val}', {address}, '{timestamp_str}', {inuse})")

        f.write(",\n".join(values) + ";\n\n")

        f.write("SET foreign_key_checks=1;\n")
        f.write("SET unique_checks=1;\n")
        f.write("SET autocommit=1;\n")
        f.write("COMMIT;\n")

    logger.info(f"Backup created: {backup_file} ({len(existing_ips)} records)")
    return backup_file


def rollback_operation(engine, backup_file, db_credentials, env):
    """Rollback the database to the backed up state"""
    if not os.path.exists(backup_file):
        logger.error(f"Backup file not found: {backup_file}")
        return False

    logger.info(f"Rolling back using backup file: {backup_file}")

    try:
        load_dump(engine, backup_file, db_credentials, env)
        logger.info("Rollback completed successfully")
        return True
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        return False


def get_rollback_confirmation():
    """Get user confirmation for rollback operation"""
    while True:
        response = input("\nDo you want to rollback the changes? (yes/no): ").strip().lower()

        if response in ['yes', 'y']:
            return True
        elif response in ['no', 'n']:
            return False
        else:
            print("Please enter 'yes' or 'no'")


def display_rollback_info(backup_file):
    """Display information about the rollback operation"""
    print("\n" + "="*60)
    print("ROLLBACK INFORMATION")
    print("="*60)
    print(f"Backup file: {backup_file}")

    if os.path.exists(backup_file):
        # Read backup file to get some info
        with open(backup_file, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if line.startswith('-- Region:'):
                    print(f"Backup region: {line.split(':')[1].strip()}")
                elif line.startswith('-- Backup Date:'):
                    print(f"Backup date: {line.split(':', 1)[1].strip()}")
                elif line.startswith('-- Total Records:'):
                    print(f"Records to restore: {line.split(':')[1].strip()}")

    print("="*60)


def main():
    parser = argparse.ArgumentParser(description='Bulk load IPs into API database using MySQL dump')
    parser.add_argument('--env', default='dev', help='Environment (dev/stage/prod)')
    parser.add_argument('--api_region', required=True, help='API AWS region to fill IPs')
    parser.add_argument('--db_region', help='RDS instance region')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompt')
    parser.add_argument('--rollback', help='Rollback using specified backup file')
    parser.add_argument('--backup-dir', help='Directory to store backup files')

    args = parser.parse_args()
    if args.env != 'local' and not args.db_region:
        parser.error("Please provide --db_region when not running locally")

    db_credentials = get_db_credentials(args.env, args.db_region)
    db_host = DB_CONFIGS[args.env]
    username = list(db_credentials.keys())[0]
    password = db_credentials[username]

    db_url = f"mysql+pymysql://{username}:{password}@{db_host}"
    engine = create_engine(db_url)

    # Handle rollback operation
    if args.rollback:
        if not validate_region(engine, args.api_region):
            logger.error(f"Region {args.api_region} not found in database")
            return

        display_rollback_info(args.rollback)

        if not args.force:
            if not get_rollback_confirmation():
                print("Rollback cancelled by user.")
                return

        success = rollback_operation(engine, args.rollback, db_credentials, args.env)
        if success:
            print("Rollback completed successfully!")
        else:
            print("Rollback failed!")
        return

    # Normal operation
    expanded_network = '172.18.0.0/15'
    current_network = '172.18.0.0/16'

    backup_file = None
    try:
        if not validate_region(engine, args.api_region):
            logger.error(f"Region {args.api_region} not found in database")
            return

        logger.info("Calculating IP addresses...")
        new_ips = get_expanded_network_ips(expanded_network, current_network)

        if len(new_ips) == 0:
            print("No new IP addresses to add. The expanded network doesn't contain any new IPs.")
            return

        # Create backup before making changes
        backup_file = create_backup(engine, args.api_region, args.backup_dir)
        if backup_file:
            print(f"\nBackup created: {backup_file}")
            print("You can use this file to rollback if needed with:")
            print(f"python {os.path.basename(__file__)} --env {args.env} --api_region {args.api_region} --rollback {backup_file}")
            if args.db_region:
                print(f" --db_region {args.db_region}")

        # Display preview
        display_operation_preview(expanded_network, current_network, args.api_region, args.env, new_ips)

        # Get user confirmation unless --force is used
        if not args.force:
            while True:
                confirmation = get_user_confirmation()
                if confirmation == 'show-sample':
                    show_sample_sql(args.api_region, new_ips)
                    continue
                elif confirmation:
                    break
                else:
                    print("Operation cancelled by user.")
                    return

        logger.info(f"Generating dump file for {len(new_ips)} addresses...")
        dump_file = generate_dump_file(args.api_region, new_ips)

        logger.info("Loading dump into database...")
        try:
            load_dump(engine, dump_file, db_credentials, args.env)
            logger.info("Import completed successfully")

            # Keep backup file for potential rollback
            if backup_file:
                print(f"\nOperation completed successfully!")
                print(f"Backup preserved at: {backup_file}")
                print("To rollback this operation, run:")
                print(f"python {os.path.basename(__file__)} --env {args.env} --api_region {args.api_region} --rollback {backup_file}")
                if args.db_region:
                    print(f" --db_region {args.db_region}")

        except Exception as e:
            logger.error(f"Import failed: {e}")

            if backup_file:
                print(f"\nImport failed! Rollback is available.")
                display_rollback_info(backup_file)

                if not args.force:
                    if get_rollback_confirmation():
                        success = rollback_operation(engine, backup_file, db_credentials, args.env)
                        if success:
                            print("Rollback completed successfully!")
                        else:
                            print("Rollback failed! Manual intervention required.")
                            print(f"Backup file available at: {backup_file}")
                else:
                    print(f"Backup file available for manual rollback at: {backup_file}")

            raise

    finally:
        if 'dump_file' in locals():
            os.unlink(dump_file)
