import boto3
import os
import sys
import argparse
from datetime import datetime
import requests

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
DNS_RECORD_NAME = os.getenv("PROD_DNS_RECORD_NAME")

BACKUP_VAULT_NAME = os.getenv("BACKUP_VAULT_NAME", "disaster-recovery-vault")

AZ_PRIMARY = os.getenv("PROD_AZ_PRIMARY")
AZ_SECONDARY = os.getenv("PROD_AZ_SECONDARY")
AZ_TERTIARY = os.getenv("PROD_AZ_TERTIARY")

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")

# AWS Clients
rds = boto3.client("rds", region_name=AWS_REGION)
backup = boto3.client("backup", region_name=AWS_REGION)
route53 = boto3.client("route53", region_name=AWS_REGION)


# =============================================================
# Backup
# =============================================================
def get_latest_backup_snapshot():
    print(f"🔍 Searching AWS Backup vault '{BACKUP_VAULT_NAME}'...")

    response = backup.list_recovery_points_by_backup_vault(
        BackupVaultName=BACKUP_VAULT_NAME
    )

    recovery_points = [
        rp for rp in response.get("RecoveryPoints", [])
        if rp.get("ResourceType") == "Aurora"
        and DB_CLUSTER_IDENTIFIER in rp.get("ResourceArn", "")
    ]

    if not recovery_points:
        print(f"❌ No backups found for '{DB_CLUSTER_IDENTIFIER}'")
        sys.exit(1)

    latest = sorted(recovery_points, key=lambda x: x["CreationDate"], reverse=True)[0]

    print(f"✅ Using snapshot: {latest['RecoveryPointArn']}")
    print(f"🕒 Created: {latest['CreationDate']}")

    return latest["RecoveryPointArn"]


# =============================================================
# Cluster
# =============================================================
def check_existing_cluster():
    try:
        clusters = rds.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
        if clusters["DBClusters"]:
            print(f"⚠️ Cluster '{DB_CLUSTER_IDENTIFIER}' already exists.")
            return True
    except rds.exceptions.DBClusterNotFoundFault:
        return False
    return False


def restore_cluster(snapshot_arn):
    print("🚀 Restoring cluster...")

    rds.restore_db_cluster_from_snapshot(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        SnapshotIdentifier=snapshot_arn,
        Engine=DB_ENGINE,
        EngineVersion=DB_ENGINE_VERSION,
        DBSubnetGroupName=DB_SUBNET_GROUP_NAME,
        VpcSecurityGroupIds=[VPC_SECURITY_GROUP_ID],
        DeletionProtection=False,
        CopyTagsToSnapshot=True
    )

    waiter = rds.get_waiter("db_cluster_available")
    waiter.wait(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)

    print("✅ Cluster is available")


# =============================================================
# Instances
# =============================================================
def create_instances(writer_az, reader_az):
    print(f"🛠️ Creating WRITER in {writer_az}...")
    rds.create_db_instance(
        DBInstanceIdentifier=DB_WRITER_IDENTIFIER,
        DBInstanceClass=DB_INSTANCE_CLASS,
        Engine=DB_ENGINE,
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        AvailabilityZone=writer_az,
        PubliclyAccessible=False
    )

    print(f"🛠️ Creating READER in {reader_az}...")
    rds.create_db_instance(
        DBInstanceIdentifier=DB_READER_IDENTIFIER,
        DBInstanceClass=DB_INSTANCE_CLASS,
        Engine=DB_ENGINE,
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        AvailabilityZone=reader_az,
        PubliclyAccessible=False
    )

    waiter = rds.get_waiter("db_instance_available")
    waiter.wait(DBInstanceIdentifier=DB_WRITER_IDENTIFIER)
    waiter.wait(DBInstanceIdentifier=DB_READER_IDENTIFIER)

    print("✅ Instances are available")


# =============================================================
# DNS
# =============================================================
def update_dns_record():
    print("🌐 Updating DNS...")

    cluster = rds.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
    endpoint = cluster["DBClusters"][0]["Endpoint"]

    route53.change_resource_record_sets(
        HostedZoneId=HOSTED_ZONE_ID,
        ChangeBatch={
            "Changes": [{
                "Action": "UPSERT",
                "ResourceRecordSet": {
                    "Name": DNS_RECORD_NAME,
                    "Type": "CNAME",
                    "TTL": 60,
                    "ResourceRecords": [{"Value": endpoint}]
                }
            }]
        }
    )

    print(f"✅ DNS → {endpoint}")


# =============================================================
# Slack
# =============================================================
def send_slack_notification():
    try:
        cluster = rds.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)["DBClusters"][0]
        instances = rds.describe_db_instances()

        writer_endpoint = cluster["Endpoint"]
        reader_endpoint = cluster.get("ReaderEndpoint", "N/A")

        writer_instance = "N/A"
        reader_instance = "N/A"

        for db in instances["DBInstances"]:
            if db["DBInstanceIdentifier"] == DB_WRITER_IDENTIFIER:
                writer_instance = db["Endpoint"]["Address"]
            if db["DBInstanceIdentifier"] == DB_READER_IDENTIFIER:
                reader_instance = db["Endpoint"]["Address"]

        payload = {
            "blocks": [
                {"type": "section",
                 "text": {"type": "mrkdwn",
                          "text": ":white_check_mark: *PROD RDS DR Restore Completed!*"}},
                {"type": "divider"},
                {"type": "section",
                 "fields": [
                     {"type": "mrkdwn", "text": f"*Cluster:* {DB_CLUSTER_IDENTIFIER}"},
                     {"type": "mrkdwn", "text": f"*Region:* {AWS_REGION}"}
                 ]},
                {"type": "section",
                 "fields": [
                     {"type": "mrkdwn", "text": f"*Writer Endpoint:*\n{writer_endpoint}"},
                     {"type": "mrkdwn", "text": f"*Reader Endpoint:*\n{reader_endpoint}"}
                 ]},
                {"type": "section",
                 "fields": [
                     {"type": "mrkdwn", "text": f"*Writer Instance:*\n{writer_instance}"},
                     {"type": "mrkdwn", "text": f"*Reader Instance:*\n{reader_instance}"}
                 ]}
            ]
        }

        requests.post(SLACK_WEBHOOK, json=payload)

    except Exception as e:
        print(f"⚠️ Slack failed: {e}")


# =============================================================
# Destroy
# =============================================================
def destroy_cluster():
    print("⚠️ Destroying cluster...")

    if os.getenv("CONFIRM_DESTROY", "NO").upper() != "YES":
        print("❌ Confirmation failed")
        sys.exit(0)

    rds.delete_db_instance(DBInstanceIdentifier=DB_READER_IDENTIFIER, SkipFinalSnapshot=True)
    rds.delete_db_instance(DBInstanceIdentifier=DB_WRITER_IDENTIFIER, SkipFinalSnapshot=True)

    waiter = rds.get_waiter("db_instance_deleted")
    waiter.wait(DBInstanceIdentifier=DB_READER_IDENTIFIER)
    waiter.wait(DBInstanceIdentifier=DB_WRITER_IDENTIFIER)

    rds.delete_db_cluster(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER, SkipFinalSnapshot=True)

    print("✅ Cluster deleted")


# =============================================================
# Main
# =============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", required=True, choices=["create", "destroy"])
    parser.add_argument("--writer_az", required=True)
    parser.add_argument("--reader_az", required=True)
    parser.add_argument("--update_dns", required=True)

    args = parser.parse_args()

    az_map = {
        "primary-az": AZ_PRIMARY,
        "secondary-az": AZ_SECONDARY,
        "tertiary-az": AZ_TERTIARY
    }

    writer_az = az_map[args.writer_az]
    reader_az = az_map[args.reader_az]

    if args.action == "create":
        if check_existing_cluster():
            sys.exit(0)

        snapshot = get_latest_backup_snapshot()
        restore_cluster(snapshot)
        create_instances(writer_az, reader_az)

        if args.update_dns == "true":
            update_dns_record()

        send_slack_notification()

    elif args.action == "destroy":
        destroy_cluster()

    print("✅ Production DR completed")
