import * as glue from 'aws-cdk-lib/aws-glue';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as cdk from 'aws-cdk-lib';

export class GlueJobStack extends cdk.Stack {
    constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        // S3 bucket references
        const rawDataBucket = s3.Bucket.fromBucketName(this, 'RawDataBucket', 'my-iot-data-bucket');
        const transformedDataBucket = s3.Bucket.fromBucketName(this, 'TransformedDataBucket', 'sdk-signalIoT');

        // Glue job role
        const glueRole = new iam.Role(this, 'GlueJobRole', {
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')
            ]
        });

        // Grant permissions to S3 buckets
        rawDataBucket.grantRead(glueRole);
        transformedDataBucket.grantWrite(glueRole);

        // Glue job definition
        new glue.CfnJob(this, 'IotDataTransformJob', {
            role: glueRole.roleArn,
            command: {
                name: 'glueetl',
                scriptLocation: 's3://sdk-signalIoT/glue_transform.py',
                pythonVersion: '3'
            },
            defaultArguments: {
                '--TempDir': 's3://sdk-signalIoT/temp/'
            },
            glueVersion: '2.0',
            maxCapacity: 2
        });
    }
}
