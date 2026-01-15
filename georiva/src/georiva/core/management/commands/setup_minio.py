import json

import boto3
from botocore.exceptions import ClientError
from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Configures MinIO bucket notifications and public read access'
    
    def handle(self, *args, **kwargs):
        # 1. Safety Check: Only run if using S3/MinIO backend
        if settings.GEORIVA_STORAGE_BACKEND != 's3':
            self.stdout.write(self.style.WARNING(
                f"Skipping MinIO setup: GEORIVA_STORAGE_BACKEND is set to '{settings.GEORIVA_STORAGE_BACKEND}'"
            ))
            return
        
        self.stdout.write("Connecting to MinIO/S3...")
        
        # 2. Reuse existing credentials from settings.py
        s3 = boto3.client(
            's3',
            endpoint_url=settings.AWS_S3_ENDPOINT_URL,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_S3_REGION_NAME
        )
        
        bucket_name = settings.AWS_STORAGE_BUCKET_NAME
        
        # 3. Ensure Bucket Exists
        try:
            s3.head_bucket(Bucket=bucket_name)
            self.stdout.write(f"Bucket '{bucket_name}' found.")
        except ClientError:
            self.stdout.write(f"Bucket '{bucket_name}' not found. Creating...")
            s3.create_bucket(Bucket=bucket_name)
        
        # 4. Set Public Read Policy
        public_read_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"]
                }
            ]
        }
        
        try:
            s3.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(public_read_policy)
            )
            self.stdout.write(self.style.SUCCESS(
                f"Successfully set public read policy on '{bucket_name}'!"
            ))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to set bucket policy: {e}"))
        
        # 5. Configure Notification
        notification_config = {
            'QueueConfigurations': [
                {
                    'Id': 'GeoRivaWebhook',
                    'QueueArn': settings.MINIO_WEBHOOK_ARN,
                    'Events': ['s3:ObjectCreated:*']
                }
            ]
        }
        
        try:
            s3.put_bucket_notification_configuration(
                Bucket=bucket_name,
                NotificationConfiguration=notification_config
            )
            self.stdout.write(self.style.SUCCESS(
                f"Successfully linked Webhook ({settings.MINIO_WEBHOOK_ARN}) to '{bucket_name}'!"
            ))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to set notification: {e}"))
