import boto3
import os
import sys
import argparse

# =============================================================
# Configuration (All values come from workflow environment)
# =============================================================
AWS_REGION = "eu-central-2"  # Region fixed by design
DB_CLUSTER_IDENTIFIER = os.getenv("DB_CLUSTER_IDENTIFIER")
DB_INSTANCE_IDENTIFIER = os.getenv("DB_INSTANCE_IDENTIFIER")
DB_ENGINE = os.getenv("DB_ENGINE")
DB_ENGINE_VERSION = os.getenv("DB_ENGINE_VERSION")
DB_INSTANCE_CLASS = os.getenv("DB_INSTANCE_CLASS")
DB_SUBNET_GROUP_NAME = os.getenv("DB_SUBNET_GROUP_NAME")
VPC_SECURITY_GROUP_ID = os.getenv("VPC_SECURITY_GROUP_ID")
HOSTED_ZONE_ID = os.getenv("HOSTED_ZONE_ID")
DNS_RECORD_NAME = os.getenv("DNS_RECORD_NAME")

AZ_PRIMARY = os.getenv("AZ_PRIMARY")
AZ_SECONDARY = os.getenv("AZ_SECONDARY")
AZ_TERTIARY = os.getenv("AZ_TERTIARY")

# Initialize clients
rds = boto3.client("rds", region_name=AWS_REGION)
backup = boto3.client("backup", region_name=AWS_REGION)
route53 = boto3.client("route53", region_name=AWS_REGION)
sts = boto3.client("sts")
AWS_ACCOUNT_ID = sts.get_caller_identity()["Account"]

# =============================================================
# Utility Functions
# =============================================================

def get_latest_backup_snapshot():
    """Fetch the latest Aurora snapshot from AWS Backup Vault."""
    print(f"🔍 Searching AWS Backup in region {AWS_REGION} for latest Aurora snapshot...")

    resource_arn = f"arn:aws:rds:{AWS_REGION}:{AWS_ACCOUNT_ID}:cluster:{DB_CLUSTER_IDENTIFIER}"

    try:
        response = backup.list_recovery_points_by_resource(ResourceArn=resource_arn)
    except Exception as e:
        print(f"❌ Error retrieving recovery points: {e}")
        sys.exit(1)

    recovery_points = response.get("RecoveryPoints", [])
    if not recovery_points:
        print("❌ No recovery points found for this resource in AWS Backup vault.")
        sys.exit(1)

    latest = sorted(recovery_points, key=lambda x: x["CreationDate"], reverse=True)[0]
    snapshot_arn = latest["RecoveryPointArn"]
    print(f"✅ Latest snapshot ARN: {snapshot_arn}")
    print(f"🕓 Created on: {latest['CreationDate']}")
    return snapshot_arn


def restore_cluster_from_snapshot(snapshot_arn, az_choice):
    """Restore Aurora cluster and instance from AWS Backup snapshot."""
    az_map = {
        "primary-az": AZ_PRIMARY,
        "secondary-az": AZ_SECONDARY,
        "tertiary-az": AZ_TERTIARY
    }

    target_az = az_map.get(az_choice)
    if not target_az:
        print("❌ Invalid Availability Zone choice. Exiting.")
        sys.exit(1)

    print(f"🚀 Restoring Aurora cluster in {target_az} from snapshot {snapshot_arn}")

    try:
        # Restore DB Cluster
        rds.restore_db_cluster_from_snapshot(
            DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
            SnapshotIdentifier=snapshot_arn,
            Engine=DB_ENGINE,
            EngineVersion=DB_ENGINE_VERSION,
            DBSubnetGroupName=DB_SUBNET_GROUP_NAME,
            VpcSecurityGroupIds=[VPC_SECURITY_GROUP_ID],
            AvailabilityZones=[target_az],
            DeletionProtection=False,
            CopyTagsToSnapshot=True
        )

        print("⏳ Waiting for DB cluster to become available...")
        waiter = rds.get_waiter("db_cluster_available")
        waiter.wait(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
        print("✅ DB cluster is now available.")

        # Create DB instance inside the cluster
        print("🛠️ Creating DB instance inside the cluster...")
        rds.create_db_instance(
            DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=DB_INSTANCE_CLASS,
            Engine=DB_ENGINE,
            DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
            PubliclyAccessible=False,
        )

        print("⏳ Waiting for DB instance to become available...")
        instance_waiter = rds.get_waiter("db_instance_available")
        instance_waiter.wait(DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER)
        print("✅ DB instance is now available.")

        # Post-creation verification
        print_post_restore_info()

    except Exception as e:
        print(f"❌ Error during restore: {e}")
        sys.exit(1)


def destroy_dr_cluster():
    """Delete the DR cluster and instance cleanly."""
    print("💥 Destroying DR cluster and instance...")

    try:
        rds.delete_db_instance(
            DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
            SkipFinalSnapshot=True
        )
        print("⏳ Waiting for DB instance to be deleted...")
        instance_waiter = rds.get_waiter("db_instance_deleted")
        instance_waiter.wait(DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER)
        print("✅ DB instance deleted successfully.")

        rds.delete_db_cluster(
            DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
            SkipFinalSnapshot=True
        )
        print("⏳ Waiting for DB cluster to be deleted...")
        cluster_waiter = rds.get_waiter("db_cluster_deleted")
        cluster_waiter.wait(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
        print("✅ DB cluster deleted successfully.")

    except Exception as e:
        print(f"❌ Error during deletion: {e}")
        sys.exit(1)


def update_dns_record():
    """Update Route53 record to point to the new cluster endpoint."""
    print("🌐 Updating Route53 DNS record...")

    try:
        cluster_info = rds.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
        endpoint = cluster_info["DBClusters"][0]["Endpoint"]

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

        print(f"✅ DNS record updated successfully to {endpoint}")

    except Exception as e:
        print(f"❌ Error updating DNS record: {e}")
        sys.exit(1)


def check_existing_cluster():
    """Check if the DR cluster already exists before restore."""
    try:
        clusters = rds.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
        if clusters["DBClusters"]:
            print(f"⚠️ Cluster '{DB_CLUSTER_IDENTIFIER}' already exists in {AWS_REGION}. Skipping creation.")
            return True
    except rds.exceptions.DBClusterNotFoundFault:
        return False
    except Exception as e:
        print(f"❌ Error checking cluster existence: {e}")
        sys.exit(1)
    return False


def print_post_restore_info():
    """Print important endpoints and identifiers after successful restore."""
    print("\n🔎 Post-Restore Verification:")

    try:
        cluster = rds.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)["DBClusters"][0]
        cluster_endpoint = cluster["Endpoint"]
        reader_endpoint = cluster.get("ReaderEndpoint", "N/A")

        print(f"✅ Cluster Endpoint: {cluster_endpoint}")
        print(f"📚 Reader Endpoint:  {reader_endpoint}")
        print(f"🗂️  Cluster ID:       {DB_CLUSTER_IDENTIFIER}")
        print(f"💡 Instance ID:      {DB_INSTANCE_IDENTIFIER}")
        print(f"🌍 Region:           {AWS_REGION}")

        print("\nYou can now connect to the DR cluster endpoint using the existing DB credentials from the snapshot.")
    except Exception as e:
        print(f"⚠️ Unable to fetch post-restore info: {e}")


# =============================================================
# Main Execution
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
            print("⚠️ Skipping restore because the cluster already exists.")
            sys.exit(0)
        snapshot_arn = get_latest_backup_snapshot()
        restore_cluster_from_snapshot(snapshot_arn, args.az_choice)
        if args.update_dns == "true":
            update_dns_record()

    elif args.action == "destroy":
        destroy_dr_cluster()

    print("✅ DR staging database operation completed successfully.")
