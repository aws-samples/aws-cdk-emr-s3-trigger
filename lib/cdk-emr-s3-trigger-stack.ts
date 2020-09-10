/* 
 * MIT No Attribution
 * 
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as s3deployment from '@aws-cdk/aws-s3-deployment';
import * as lambda from '@aws-cdk/aws-lambda';
import * as iam from '@aws-cdk/aws-iam';
import { S3EventSource } from '@aws-cdk/aws-lambda-event-sources';
import * as path from 'path';

interface Props extends cdk.StackProps {
  sourceBucketName: string,
  destinationBucketName: string,
  jobBucketName: string,
  jobFileName: string
}

export class CdkEmrS3TriggerStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props: Props) {
    super(scope, id, props);
    const sourceBucket = new s3.Bucket(this, 'SourceBucket', {
      bucketName: props.sourceBucketName
    });
    const destinationBucket = new s3.Bucket(this, 'DestinationBucket', {
      bucketName: props.destinationBucketName
    });
    const jobBucket = new s3.Bucket(this, 'JobBucket', {
      bucketName: props.jobBucketName
    });
    new s3deployment.BucketDeployment(this, 'UploadSimpleSparkJob', {
      destinationBucket: jobBucket,
      sources: [s3deployment.Source.asset('demo/')]
    })
    
    const helloEmrFunction = new lambda.Function(this, 'HelloEmrFunction', {
      runtime: lambda.Runtime.PYTHON_3_8,
      handler: 'hello_emr.lambda_handler',
      code: lambda.Code.fromAsset(path.join(__dirname, 'lambda-handler')),
      environment: {
        DESTINATION_BUCKET_NAME: props.destinationBucketName,
        JOB_BUCKET_NAME: props.jobBucketName,
        JOB_FILE_NAME: props.jobFileName
      }
    });
    helloEmrFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        "elasticmapreduce:RunJobFlow"
      ],
      resources: ['*']
    }));
    helloEmrFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        "iam:PassRole"
      ],
      resources: [`arn:aws:iam::${this.account}:role/EMR_DefaultRole`,
                  `arn:aws:iam::${this.account}:role/EMR_EC2_DefaultRole`]
    }));

    helloEmrFunction.addEventSource(new S3EventSource(sourceBucket, {
      events: [s3.EventType.OBJECT_CREATED]
    }))
  }
}
