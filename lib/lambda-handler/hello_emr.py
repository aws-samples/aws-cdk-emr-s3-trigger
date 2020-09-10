################################################################################
#
# MIT No Attribution
#
# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
################################################################################
import json
import boto3
import os
import time

emr = boto3.client('emr')

dest_bucket_name = os.environ.get('DESTINATION_BUCKET_NAME')
job_bucket_name = os.environ.get('JOB_BUCKET_NAME')
job_file_name = os.environ.get('JOB_FILE_NAME')

job_file_path = f's3://{job_bucket_name}/{job_file_name}'

def lambda_handler(event, context):
    print(event)

    input_file_path = get_upload_file_path(event)
    dest_file_path = f's3://{dest_bucket_name}/{int(time.time())}'

    response = emr.run_job_flow(
        Name="SampleCluster",
        ReleaseLabel='emr-5.23.0',
        Applications=[
            {
                'Name': 'Hadoop'
            },
            {
                'Name': 'Hive'
            },
            {
                'Name': 'Spark'
            },
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'r3.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Core nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'r3.xlarge',
                    'InstanceCount': 4,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 100
                                },
                                'VolumesPerInstance': 1
                            },
                        ],
                        'EbsOptimized': False
                    },
                },
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False
            #'Ec2SubnetId': ec2SubnetId,
            #'EmrManagedMasterSecurityGroup': master_sg,
            #'EmrManagedSlaveSecurityGroup': slave_sg,
            #'ServiceAccessSecurityGroup': service_sg,
        },
        Steps=[
            {
                'Name': 'Spark application',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                    'spark-submit',
                    '--class','com.aws.sample.SparkSimpleJob',
                    job_file_path,
                    input_file_path,
                    dest_file_path
                    ]
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
    )

    # print(response)

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }

def get_upload_file_path(event):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    return f's3://{bucket}/{key}'
