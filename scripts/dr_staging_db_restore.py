import boto3
import argparse
import os
import sys

AWS_REGION = os.getenv("AWS_REGION")

DB_CLUSTER_IDENTIFIER = os.getenv("DB_CLUSTER_IDENTIFIER")
DB_INSTANCE_IDENTIFIER = os.getenv("DB_INSTANCE_IDENTIFIER")
DB_ENGINE = os.getenv("DB_ENGINE")
DB_ENGINE_VERSION = os.getenv("DB_ENGINE_VERSION")
DB_INSTANCE_CLASS = os.getenv("DB_INSTANCE_CLASS")
SNAPSHOT_TAG_KEY = os.getenv("SNAPSHOT_TAG_KEY")
SNAPSHOT_TAG_VALUE = os.getenv("SNAPSHOT_TAG_VALUE")
DB_SUBNET_GROUP_NAME = os.getenv("DB_SUBNET_GROUP_NAME")
VPC_SECURITY_GROUP_ID = os.getenv("VPC_SECURITY_GROUP_ID")
HOSTED_ZONE_ID = os.getenv("HOSTED_ZONE_ID")
DNS_RECORD_NAME = os.getenv("DNS_RECORD_NAME")

AZ_MAPPING = {
    "primary-az": os.getenv("AZ_PRIMARY"),
    "secondary-az": os.getenv("AZ_SECONDARY"),
    "tertiary-az": os.getenv("AZ_TERTIARY"),
}

rds_client = boto3.client("rds", region_name=AWS_REGION)
route53_client = boto3.client("route53", region_name=AWS_REGION)

def get_latest_snapshot():
    print(f"🔍 Searching for snapshots tagged {SNAPSHOT_TAG_KEY}={SNAPSHOT_TAG_VALUE}")
    snapshots = rds_client.describe_db_cluster_snapshots(SnapshotType="manual")["DBClusterSnapshots"]

    filtered = [
        s for s in snapshots
        if any(
            t["Key"] == SNAPSHOT_TAG_KEY and t["Value"] == SNAPSHOT_TAG_VALUE
            for t in s.get("TagList", [])
        )
    ]

    if not filtered:
        print("❌ No snapshots found matching tag criteria.")
        sys.exit(1)

    latest = sorted(filtered, key=lambda x: x["SnapshotCreateTime"], reverse=True)[0]
    print(f"✅ Latest snapshot found: {latest['DBClusterSnapshotIdentifier']}")
    return latest["DBClusterSnapshotIdentifier"]

def create_dr_cluster(az_choice):
    snapshot_id = get_latest_snapshot()
    print("🚀 Restoring DR Aurora cluster from snapshot...")

    cluster = rds_client.restore_db_cluster_from_snapshot(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        SnapshotIdentifier=snapshot_id,
        Engine=DB_ENGINE,
        EngineVersion=DB_ENGINE_VERSION,
        DBSubnetGroupName=DB_SUBNET_GROUP_NAME,
        VpcSecurityGroupIds=[VPC_SECURITY_GROUP_ID],
        Tags=[{"Key": "CreatedBy", "Value": "GitHubActions"}]
    )
    print(f"✅ Cluster restoration initiated: {cluster['DBCluster']['DBClusterIdentifier']}")

    az = AZ_MAPPING.get(az_choice)
    if not az:
        print(f"❌ Invalid AZ choice: {az_choice}")
        sys.exit(1)

    print(f"🧭 Creating DB instance in AZ: {az}")
    instance = rds_client.create_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        Engine=DB_ENGINE,
        DBInstanceClass=DB_INSTANCE_CLASS,
        AvailabilityZone=az
    )
    print(f"✅ Instance creation initiated: {instance['DBInstance']['DBInstanceIdentifier']}")

# ---------------------------------------------------------------------
# 🧹 Destroy DR Cluster and Instance
# ---------------------------------------------------------------------
def delete_dr_cluster():
    print("🧹 Deleting DR cluster and instance...")
    try:
        rds_client.delete_db_instance(
            DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
            SkipFinalSnapshot=True
        )
        print("🧩 Instance deletion initiated.")
    except rds_client.exceptions.DBInstanceNotFoundFault:
        print("⚠️ No instance found; skipping instance deletion.")

    try:
        rds_client.delete_db_cluster(
            DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
            SkipFinalSnapshot=True
        )
        print("✅ Cluster deletion initiated.")
    except rds_client.exceptions.DBClusterNotFoundFault:
        print("⚠️ No cluster found; skipping cluster deletion.")


def update_dns_record():
    print("🌐 Updating Route 53 DNS record...")
    clusters = rds_client.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_IDENTIFIER)
    endpoint = clusters["DBClusters"][0]["Endpoint"]
    print(f"🔗 New cluster endpoint: {endpoint}")

    change_batch = {
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

    response = route53_client.change_resource_record_sets(
        HostedZoneId=HOSTED_ZONE_ID,
        ChangeBatch=change_batch
    )
    print(f"✅ DNS update initiated (Change ID: {response['ChangeInfo']['Id']})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", required=True, choices=["create", "destroy"])
    parser.add_argument("--az_choice", required=True, choices=list(AZ_MAPPING.keys()))
    parser.add_argument("--update_dns", required=True, choices=["true", "false"])
    args = parser.parse_args()

    if args.action == "create":
        create_dr_cluster(args.az_choice)
    elif args.action == "destroy":
        delete_dr_cluster()

    if args.update_dns == "true" and args.action == "create":
        update_dns_record()
