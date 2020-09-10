#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import * as fs from 'fs';
import * as path from 'path';
import { CdkEmrS3TriggerStack } from '../lib/cdk-emr-s3-trigger-stack';

const app = new cdk.App();

const manifest = JSON.parse(fs.readFileSync(`${path.join(__dirname,'../manifest.json')}`, 'utf8'));
console.log(`source bucket name      : ${manifest.config.bucket.source}`);
console.log(`destination bucket name : ${manifest.config.bucket.destination}`);
console.log(`job bucket name         : ${manifest.config.bucket.job}`);
console.log(`job file name           : ${manifest.config.job.file}`)
console.log(`deployment version      : ${manifest.config.deploy.version}`);
console.log(`deployment environment  : ${manifest.config.deploy.environment}`);

new CdkEmrS3TriggerStack(app, `CdkEmrS3TriggerStack-${manifest.config.deploy.version}-${manifest.config.deploy.environment}`, {
  sourceBucketName: manifest.config.bucket.source,
  destinationBucketName: manifest.config.bucket.destination,
  jobBucketName: manifest.config.bucket.job,
  jobFileName: manifest.config.job.file
});
