"""
Management command to configure GeoRiva's MinIO/S3 buckets.

Sets up:
    georiva-incoming   → webhook notifications (triggers ingestion)
    georiva-sources    → webhook notifications (triggers ingestion)
    georiva-archive    → no notifications, private
    georiva-assets     → public read access (serves processed data)

Usage:
    python manage.py setup_minio
"""

import json

import boto3
from botocore.exceptions import ClientError
from django.conf import settings
from django.core.management.base import BaseCommand

# Bucket-specific configuration
BUCKET_CONFIGS = {
    "incoming": {
        "public_read": False,
        "notify_on_create": True,
        "description": "User-uploaded raw data",
    },
    "sources": {
        "public_read": False,
        "notify_on_create": True,
        "description": "Plugin-collected data",
    },
    "archive": {
        "public_read": False,
        "notify_on_create": False,
        "description": "Raw data preservation",
    },
    "assets": {
        "public_read": True,
        "notify_on_create": False,
        "description": "Processed datasets (public)",
    },
}


class Command(BaseCommand):
    help = "Configure GeoRiva MinIO buckets, policies, and notifications"
    
    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be done without making changes",
        )
    
    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        
        if settings.GEORIVA_STORAGE_BACKEND != "s3":
            self.stdout.write(self.style.WARNING(
                f"Skipping: GEORIVA_STORAGE_BACKEND is '{settings.GEORIVA_STORAGE_BACKEND}'"
            ))
            return
        
        self.stdout.write("Connecting to MinIO/S3...")
        
        s3 = boto3.client(
            "s3",
            endpoint_url=settings.AWS_S3_ENDPOINT_URL,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_S3_REGION_NAME,
        )
        
        buckets = getattr(settings, "GEORIVA_BUCKETS", {
            "incoming": "georiva-incoming",
            "sources": "georiva-sources",
            "archive": "georiva-archive",
            "assets": "georiva-assets",
        })
        
        webhook_arn = getattr(settings, "MINIO_WEBHOOK_ARN", None)
        
        for bucket_type, bucket_name in buckets.items():
            config = BUCKET_CONFIGS.get(bucket_type, {})
            
            self.stdout.write(
                f"\n{'[DRY RUN] ' if dry_run else ''}"
                f"Setting up: {bucket_name} ({config.get('description', '')})"
            )
            
            # 1. Ensure bucket exists
            self._ensure_bucket(s3, bucket_name, dry_run)
            
            # 2. Set bucket policy
            if config.get("public_read"):
                self._set_public_read_policy(s3, bucket_name, dry_run)
            else:
                self._set_private_policy(s3, bucket_name, dry_run)
            
            # 3. Configure notifications
            if config.get("notify_on_create") and webhook_arn:
                self._set_webhook_notification(
                    s3, bucket_name, webhook_arn, dry_run
                )
            elif config.get("notify_on_create") and not webhook_arn:
                self.stdout.write(self.style.WARNING(
                    f"  ⚠ Notifications requested but MINIO_WEBHOOK_ARN not set"
                ))
        
        self.stdout.write(self.style.SUCCESS("\nMinIO setup complete."))
    
    # =========================================================================
    # Bucket creation
    # =========================================================================
    
    def _ensure_bucket(self, s3, bucket_name: str, dry_run: bool):
        try:
            s3.head_bucket(Bucket=bucket_name)
            self.stdout.write(f"  ✓ Bucket exists: {bucket_name}")
        except ClientError:
            if dry_run:
                self.stdout.write(f"  → Would create bucket: {bucket_name}")
            else:
                try:
                    s3.create_bucket(Bucket=bucket_name)
                    self.stdout.write(self.style.SUCCESS(
                        f"  + Created bucket: {bucket_name}"
                    ))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(
                        f"  ✗ Failed to create {bucket_name}: {e}"
                    ))
    
    # =========================================================================
    # Bucket policies
    # =========================================================================
    
    def _set_public_read_policy(
            self, s3, bucket_name: str, dry_run: bool
    ):
        """Allow public read access (for assets bucket)."""
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"],
                }
            ],
        }
        
        if dry_run:
            self.stdout.write(f"  → Would set public read policy")
            return
        
        try:
            s3.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(policy),
            )
            self.stdout.write(self.style.SUCCESS(
                f"  ✓ Public read policy set"
            ))
        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f"  ✗ Failed to set policy: {e}"
            ))
    
    def _set_private_policy(self, s3, bucket_name: str, dry_run: bool):
        """Remove any public policy (private bucket)."""
        if dry_run:
            self.stdout.write(f"  → Would ensure private policy")
            return
        
        try:
            s3.delete_bucket_policy(Bucket=bucket_name)
            self.stdout.write(f"  ✓ Private policy set (no public access)")
        except ClientError as e:
            # No policy to delete is fine
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("NoSuchBucketPolicy", "404"):
                self.stdout.write(f"  ✓ Already private")
            else:
                self.stdout.write(self.style.ERROR(
                    f"  ✗ Failed to set private policy: {e}"
                ))
    
    # =========================================================================
    # Webhook notifications
    # =========================================================================
    
    def _set_webhook_notification(
            self, s3, bucket_name: str, webhook_arn: str, dry_run: bool
    ):
        """
        Configure S3 event notifications for file uploads.

        Triggers on s3:ObjectCreated:* so the ingestion pipeline
        picks up new files in incoming/sources buckets.
        """
        notification_config = {
            "QueueConfigurations": [
                {
                    "Id": f"GeoRiva-{bucket_name}",
                    "QueueArn": webhook_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {
                                    "Name": "suffix",
                                    "Value": "",
                                }
                            ]
                        }
                    },
                }
            ],
        }
        
        if dry_run:
            self.stdout.write(
                f"  → Would set webhook notification → {webhook_arn}"
            )
            return
        
        try:
            s3.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=notification_config,
            )
            self.stdout.write(self.style.SUCCESS(
                f"  ✓ Webhook notification set → {webhook_arn}"
            ))
        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f"  ✗ Failed to set notification: {e}"
            ))
