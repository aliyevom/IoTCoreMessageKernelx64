import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as cdk from 'aws-cdk-lib';

export class EventBridgeLambdaStack extends cdk.Stack {
    constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        // SNS topic for notifications
        const snsTopic = new sns.Topic(this, 'IoTAlerts', {
            topicName: 'IoTAlerts'
        });

        // Lambda function for state change handling
        const stateChangeLambda = new lambda.Function(this, 'StateChangeLambda', {
            runtime: lambda.Runtime.PYTHON_3_8,
            handler: 'device_state_change_handler.lambda_handler',
            code: lambda.Code.fromAsset('lambda'),
            environment: {
                SNS_TOPIC_ARN: snsTopic.topicArn
            }
        });

        // Grant SNS publish permission to Lambda
        snsTopic.grantPublish(stateChangeLambda);

        // EventBridge rule to monitor device state changes
        const stateChangeRule = new events.Rule(this, 'StateChangeRule', {
            eventPattern: {
                source: ['aws.iot'],
                detailType: ['IoT Thing Shadow'],
                detail: {
                    'state.reported.temperature': [{ 'numeric': ['>', 30] }]
                }
            }
        });

        // Add the Lambda function as the target of the EventBridge rule
        stateChangeRule.addTarget(new targets.LambdaFunction(stateChangeLambda));
    }
}
