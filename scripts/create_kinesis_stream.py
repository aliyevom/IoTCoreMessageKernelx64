import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as cdk from 'aws-cdk-lib';

export class KinesisStreamStack extends cdk.Stack {
    constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        // Create a Kinesis Data Stream
        const iotDataStream = new kinesis.Stream(this, 'IoTDataStream', {
            streamName: 'IoTDataStream',
            shardCount: 2,  // You can scale this up for higher throughput
        });

        // Output the stream name for reference
        new cdk.CfnOutput(this, 'IoTDataStreamName', {
            value: iotDataStream.streamName,
            description: 'The name of the Kinesis Data Stream for IoT data.'
        });
    }
}
