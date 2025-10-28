import boto3
import os
import sys
import argparse
from datetime import datetime

# =============================================================
# Configuration (from workflow env)
# =============================================================
AWS_REGION = "eu-central-2"
DB_CLUSTER_IDENTIFIER = os.getenv("DB_CLUSTER_IDENTIFIER")
DB_INSTANCE_IDENTIFIER = os.getenv("DB_INSTANCE_IDENTIFIER")
DB_ENGINE = os.getenv("DB_ENGINE")
DB_ENGINE_VERSION = os.getenv("DB_ENGINE_VERSION")
DB_INSTANCE_CLASS = os.getenv("DB_INSTANCE_CLASS")
DB_SUBNET_GROUP_NAME = os.getenv("DB_SUBNET_GROUP_NAME")
VPC_SECURITY_GROUP_ID = os.getenv("VPC_SECURITY_GROUP_ID")
HOSTED_ZONE_ID = os.getenv("HOSTED_ZONE_ID")
DNS_RECORD_NAME = os.getenv("DNS_RECORD_NAME")
BACKUP_VAULT_NAME = os.getenv("BACKUP_VAULT_NAME", "disaster-recovery-vault")

AZ_PRIMARY = os.getenv("AZ_PRIMARY")
AZ_SECONDARY = os.getenv("AZ_SECONDARY")
AZ_TERTIARY = os.getenv("AZ_TERTIARY")

# AWS Clients
rds = boto3.client("rds", region_name=AWS_REGION)
backup = boto3.client("backup", region_name=AWS_REGION)
route53 = boto3.client("route53", region_name=AWS_REGION)
sts = boto3.client("sts")
AWS_ACCOUNT_ID = sts.get_caller_identity()["Account"]


# =============================================================
# Utility Functions
# =============================================================

def describe_subnet_group():
    """Describe DB subnet group for diagnostics."""
    try:
        resp = rds.describe_db_subnet_groups(DBSubnetGroupName=DB_SUBNET_GROUP_NAME)
        subnets = resp["DBSubnetGroups"][0]["Subnets"]
        print(f"🧩 Subnet Group '{DB_SUBNET_GROUP_NAME}' contains the following subnets:")
        for s in subnets:
            subnet_id = s["SubnetIdentifier"]
            az = s["SubnetAvailabilityZone"]["Name"]
            cidr = s["SubnetOutpostArn"] if "SubnetOutpostArn" in s else "N/A"
            print(f"   • Subnet {subnet_id} | AZ: {az} | CIDR: {cidr}")
    except Exception as e:
        print(f"⚠️ Unable to describe subnet group: {e}")


def get_latest_backup_snapshot():
    """Fetch the latest Aurora snapshot from AWS Backup Vault."""
    print(f"🔍 Searching AWS Backup vault '{BACKUP_VAULT_NAME}' for latest Aurora snapshot...")

    try:
        response = backup.list_recovery_points_by_backup_vault(BackupVaultName=BACKUP_VAULT_NAME)
    except Exception as e:
        print(f"❌ Error retrieving recovery points: {e}")
        sys.exit(1)

    recovery_points = [
        rp for rp in response.get("RecoveryPoints", [])
        if rp.get("ResourceType") == "Aurora"
        and DB_CLUSTER_IDENTIFIER in rp.get("ResourceArn", "")
    ]

    if not recovery_points:
        print(f"❌ No Aurora recovery points found for '{DB_CLUSTER_IDENTIFIER}'.")
        sys.exit(1)

    latest = sorted(recovery_points, key=lambda x: x["CreationDate"], reverse=True)[0]
    snapshot_arn = latest["RecoveryPointArn"]
    created_time = latest["CreationDate"].strftime("%Y-%m-%d %H:%M:%S")

    print(f"✅ Using latest Aurora snapshot ARN: {snapshot_arn}")
    print(f"🕓 Snapshot created on: {created_time}")
    return snapshot_arn


def check_existing_cluster():
    try:
        clusters = rds.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
        if clusters["DBClusters"]:
            print(f"⚠️ Cluster '{DB_CLUSTER_IDENTIFIER}' already exists. Skipping creation.")
            return True
    except rds.exceptions.DBClusterNotFoundFault:
        return False
    except Exception as e:
        print(f"❌ Error checking cluster: {e}")
        sys.exit(1)
    return False


def restore_cluster_from_snapshot(snapshot_arn, az_choice):
    az_map = {
        "primary-az": AZ_PRIMARY,
        "secondary-az": AZ_SECONDARY,
        "tertiary-az": AZ_TERTIARY
    }

    target_az = az_map.get(az_choice)
    if not target_az:
        print("❌ Invalid AZ selection.")
        sys.exit(1)

    print(f"🚀 Restoring Aurora cluster from snapshot in {target_az}...")

    describe_subnet_group()

    try:
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

        print("⏳ Waiting for cluster to become available...")
        waiter = rds.get_waiter("db_cluster_available")
        waiter.wait(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
        print("✅ Cluster is available.")

        # Explicitly enforce target AZ for instance
        print(f"🛠️ Creating DB instance in {target_az}...")
        rds.create_db_instance(
            DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=DB_INSTANCE_CLASS,
            Engine=DB_ENGINE,
            DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
            PubliclyAccessible=False,
            AvailabilityZone=target_az
        )

        print("⏳ Waiting for DB instance to be available...")
        instance_waiter = rds.get_waiter("db_instance_available")
        instance_waiter.wait(DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER)
        print(f"✅ DB instance is now available in {target_az}.")

        print_post_restore_info()

    except Exception as e:
        print(f"❌ Error during restore: {e}")
        sys.exit(1)


def destroy_dr_cluster():
    print("⚠️ WARNING: You are about to delete the DR cluster.")

    confirmation = os.getenv("CONFIRM_DESTROY", "NO").strip().upper()
    if confirmation != "YES":
        print("❌ Destruction aborted. Must confirm with YES.")
        sys.exit(0)

    try:
        print("💥 Deleting DB instance...")
        rds.delete_db_instance(
            DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
            SkipFinalSnapshot=True
        )
        instance_waiter = rds.get_waiter("db_instance_deleted")
        instance_waiter.wait(DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER)
        print("✅ DB instance deleted.")

        print("💥 Deleting DB cluster...")
        rds.delete_db_cluster(
            DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
            SkipFinalSnapshot=True
        )
        cluster_waiter = rds.get_waiter("db_cluster_deleted")
        cluster_waiter.wait(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
        print("✅ DB cluster deleted.")
    except Exception as e:
        print(f"❌ Error deleting cluster: {e}")
        sys.exit(1)


def update_dns_record():
    print("🌐 Updating Route53 DNS record...")
    try:
        cluster = rds.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
        endpoint = cluster["DBClusters"][0]["Endpoint"]

        route53.change_resource_record_sets(
            HostedZoneId=HOSTED_ZONE_ID,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": DNS_RECORD_NAME,
                            "Type": "CNAME",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": endpoint}]
                        }
                    }
                ]
            }
        )
        print(f"✅ DNS updated → {DNS_RECORD_NAME} → {endpoint}")
    except Exception as e:
        print(f"❌ Error updating DNS: {e}")
        sys.exit(1)


def print_post_restore_info():
    try:
        cluster = rds.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)["DBClusters"][0]
        cluster_endpoint = cluster["Endpoint"]
        reader_endpoint = cluster.get("ReaderEndpoint", "N/A")

        print("\n🔎 Post-Restore Info:")
        print(f"✅ Cluster Endpoint: {cluster_endpoint}")
        print(f"📚 Reader Endpoint:  {reader_endpoint}")
        print(f"🗂️ Cluster ID:       {DB_CLUSTER_IDENTIFIER}")
        print(f"💡 Instance ID:      {DB_INSTANCE_IDENTIFIER}")
        print(f"📍 Region:           {AWS_REGION}")
        print(f"📅 Restored:         {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    except Exception as e:
        print(f"⚠️ Unable to fetch restore info: {e}")


# =============================================================
# Main
# =============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aurora DR Automation Script")
    parser.add_argument("--action", required=True, choices=["create", "destroy"])
    parser.add_argument("--az_choice", required=True, choices=["primary-az", "secondary-az", "tertiary-az"])
    parser.add_argument("--update_dns", required=True, choices=["true", "false"])
    args = parser.parse_args()

    print(f"🧭 Region: {AWS_REGION}")
    print(f"🗂️ Action: {args.action}")
    print(f"🏗️ AZ Choice: {args.az_choice}")
    print(f"🌐 Update DNS: {args.update_dns}")

    if args.action == "create":
        if check_existing_cluster():
            print("⚠️ Cluster already exists — skipping restore.")
            sys.exit(0)
        snapshot_arn = get_latest_backup_snapshot()
        restore_cluster_from_snapshot(snapshot_arn, args.az_choice)
        if args.update_dns == "true":
            update_dns_record()

    elif args.action == "destroy":
        destroy_dr_cluster()

    print("✅ DR staging database operation completed successfully.")
