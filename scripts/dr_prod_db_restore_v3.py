import boto3
import os
import sys
import argparse
import requests
from datetime import datetime

# =============================================================
# Configuration
# =============================================================
AWS_REGION = "eu-central-2"

DB_CLUSTER_IDENTIFIER = os.getenv("PROD_DB_CLUSTER_IDENTIFIER")
DB_WRITER_IDENTIFIER = os.getenv("PROD_DB_WRITER_IDENTIFIER")
DB_READER_IDENTIFIER = os.getenv("PROD_DB_READER_IDENTIFIER")

DB_ENGINE = os.getenv("PROD_DB_ENGINE")
DB_ENGINE_VERSION = os.getenv("PROD_DB_ENGINE_VERSION")
DB_INSTANCE_CLASS = os.getenv("PROD_DB_CLASS")

DB_SUBNET_GROUP_NAME = os.getenv("PROD_DB_SUBNET_GROUP_NAME")
VPC_SECURITY_GROUP_ID = os.getenv("PROD_VPC_SECURITY_GROUP_ID")

HOSTED_ZONE_ID = os.getenv("PROD_HOSTED_ZONE_ID")
WRITER_DNS = os.getenv("PROD_WRITER_DNS_RECORD")
READER_DNS = os.getenv("PROD_READER_DNS_RECORD")

BACKUP_VAULT_NAME = os.getenv("BACKUP_VAULT_NAME", "disaster-recovery-vault")

AZ_PRIMARY = os.getenv("PROD_AZ_PRIMARY")
AZ_SECONDARY = os.getenv("PROD_AZ_SECONDARY")
AZ_TERTIARY = os.getenv("PROD_AZ_TERTIARY")

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")

DB_CLUSTER_PARAMETER_GROUP = os.getenv("PROD_DB_CLUSTER_PARAMETER_GROUP")

# AWS Clients
rds = boto3.client("rds", region_name=AWS_REGION)
backup = boto3.client("backup", region_name=AWS_REGION)
route53 = boto3.client("route53", region_name=AWS_REGION)

# =============================================================
# Backup
# =============================================================
def get_latest_backup_snapshot():
    response = backup.list_recovery_points_by_backup_vault(
        BackupVaultName=BACKUP_VAULT_NAME
    )

    recovery_points = [
        rp for rp in response.get("RecoveryPoints", [])
        if rp.get("ResourceType") == "Aurora"
        and DB_CLUSTER_IDENTIFIER in rp.get("ResourceArn", "")
    ]

    if not recovery_points:
        print("❌ No Aurora backups found.")
        sys.exit(1)

    latest = sorted(recovery_points, key=lambda x: x["CreationDate"], reverse=True)[0]
    return latest["RecoveryPointArn"]

# =============================================================
# Cluster
# =============================================================
def check_existing_cluster():
    try:
        clusters = rds.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
        if clusters["DBClusters"]:
            print(f"⚠️ Cluster '{DB_CLUSTER_IDENTIFIER}' already exists. Skipping restore.")
            return True
    except rds.exceptions.DBClusterNotFoundFault:
        return False
    return False


def restore_cluster(snapshot_arn):
    print("🚀 Restoring Aurora cluster...")

    restore_params = {
        "DBClusterIdentifier": DB_CLUSTER_IDENTIFIER,
        "SnapshotIdentifier": snapshot_arn,
        "Engine": DB_ENGINE,
        "EngineVersion": DB_ENGINE_VERSION,
        "DBSubnetGroupName": DB_SUBNET_GROUP_NAME,
        "VpcSecurityGroupIds": [VPC_SECURITY_GROUP_ID],
        "DeletionProtection": False,
        "CopyTagsToSnapshot": True
    }

    
    if DB_CLUSTER_PARAMETER_GROUP:
        restore_params["DBClusterParameterGroupName"] = DB_CLUSTER_PARAMETER_GROUP
        print(f"🔧 Using parameter group: {DB_CLUSTER_PARAMETER_GROUP}")

    rds.restore_db_cluster_from_snapshot(**restore_params)

    print("⏳ Waiting for cluster...")
    waiter = rds.get_waiter("db_cluster_available")
    waiter.wait(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
    print("✅ Cluster is available.")

# =============================================================
# Instances
# =============================================================
def create_instances(writer_az, reader_az):
    cluster = rds.describe_db_clusters(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER
    )["DBClusters"][0]

    engine = cluster["Engine"]

    print(f"🛠️ Creating WRITER in {writer_az}...")
    rds.create_db_instance(
        DBInstanceIdentifier=DB_WRITER_IDENTIFIER,
        DBInstanceClass=DB_INSTANCE_CLASS,
        Engine=engine,
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        AvailabilityZone=writer_az,
        PubliclyAccessible=False
    )

    print(f"🛠️ Creating READER in {reader_az}...")
    rds.create_db_instance(
        DBInstanceIdentifier=DB_READER_IDENTIFIER,
        DBInstanceClass=DB_INSTANCE_CLASS,
        Engine=engine,
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        AvailabilityZone=reader_az,
        PubliclyAccessible=False
    )

    print("⏳ Waiting for instances...")
    waiter = rds.get_waiter("db_instance_available")
    waiter.wait(DBInstanceIdentifier=DB_WRITER_IDENTIFIER)
    waiter.wait(DBInstanceIdentifier=DB_READER_IDENTIFIER)

    print("✅ Writer & Reader instances are ready.")

# =============================================================
# DNS
# =============================================================
def update_dns():
    print("🌐 Updating DNS records...")

    cluster = rds.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)["DBClusters"][0]

    writer_endpoint = cluster["Endpoint"]
    reader_endpoint = cluster.get("ReaderEndpoint")

    changes = []

    if WRITER_DNS:
        changes.append({
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": WRITER_DNS,
                "Type": "CNAME",
                "TTL": 60,
                "ResourceRecords": [{"Value": writer_endpoint}]
            }
        })

    if READER_DNS and reader_endpoint:
        changes.append({
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": READER_DNS,
                "Type": "CNAME",
                "TTL": 60,
                "ResourceRecords": [{"Value": reader_endpoint}]
            }
        })

    route53.change_resource_record_sets(
        HostedZoneId=HOSTED_ZONE_ID,
        ChangeBatch={"Changes": changes}
    )

    print("✅ DNS updated.")

# =============================================================
# Slack
# =============================================================
def send_slack():
    cluster = rds.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)["DBClusters"][0]

    payload = {
        "text": f"""
✅ *PROD Database DR Restore Completed*

*Cluster:* {DB_CLUSTER_IDENTIFIER}
*Writer Endpoint:* {cluster['Endpoint']}
*Reader Endpoint:* {cluster.get('ReaderEndpoint')}
*Region:* {AWS_REGION}
*Time:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
"""
    }

    try:
        requests.post(SLACK_WEBHOOK, json=payload)
    except Exception as e:
        print(f"⚠️ Slack notification failed: {e}")

# =============================================================
# Destroy
# =============================================================
def destroy():
    print("⚠️ Destroying cluster...")

    if os.getenv("CONFIRM_DESTROY", "NO").upper() != "YES":
        print("❌ Confirmation failed.")
        return

    rds.delete_db_instance(DBInstanceIdentifier=DB_READER_IDENTIFIER, SkipFinalSnapshot=True)
    rds.delete_db_instance(DBInstanceIdentifier=DB_WRITER_IDENTIFIER, SkipFinalSnapshot=True)

    waiter = rds.get_waiter("db_instance_deleted")
    waiter.wait(DBInstanceIdentifier=DB_READER_IDENTIFIER)
    waiter.wait(DBInstanceIdentifier=DB_WRITER_IDENTIFIER)

    rds.delete_db_cluster(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER, SkipFinalSnapshot=True)

    print("✅ Cluster destroyed.")

# =============================================================
# Main
# =============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", required=True, choices=["create", "destroy"])
    parser.add_argument("--writer_az", required=True, choices=["primary-az", "secondary-az", "tertiary-az"])
    parser.add_argument("--reader_az", required=True, choices=["primary-az", "secondary-az", "tertiary-az"])
    parser.add_argument("--update_dns", required=True, choices=["true", "false"])

    args = parser.parse_args()

    az_map = {
        "primary-az": AZ_PRIMARY,
        "secondary-az": AZ_SECONDARY,
        "tertiary-az": AZ_TERTIARY
    }

    if args.action == "create":
        if check_existing_cluster():
            sys.exit(0)

        snapshot = get_latest_backup_snapshot()
        restore_cluster(snapshot)

        create_instances(
            az_map[args.writer_az],
            az_map[args.reader_az]
        )

        if args.update_dns == "true":
            update_dns()

        send_slack()

    elif args.action == "destroy":
        destroy()

    print("✅ Production DR operation completed successfully.")
