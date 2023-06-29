#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";

import { Vpc } from "./vpc";
import { Config } from "./config";
import { PgStacInfra } from "./pgstacinfra";
import { Construct } from "constructs";
const { terminationProtection, stage, version, buildStackName, tags } =
  new Config();

export const app = new cdk.App({});

const { vpc } = new Vpc(app, buildStackName("vpc"), {
  terminationProtection,
  tags,
  natGatewayCount: stage === "prod" ? undefined : 1,
});

const pgstacInfraStackName = buildStackName('pgSTAC');
const pgstacInfra = new PgStacInfra(app, pgstacInfraStackName, {
  vpc,
  terminationProtection,
  tags,
  stage,
  version,
});

export class eodcHubRole extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    const pgstacStackArn = cdk.Fn.importValue(`${pgstacInfraStackName}-stackArn`);
    const securityGroupId = cdk.Fn.importValue(`${pgstacInfraStackName}-securityGroupId`);

    // Create the IAM role
    const eodcHubRole = new cdk.aws_iam.Role(this, 'eodcHubRole', {
      roleName: 'eodc-hub-role',
      assumedBy: new cdk.aws_iam.ArnPrincipal(`arn:aws:iam::${this.account}:role/nasa-veda-prod`),
    });

    // Grant permission to access the secret
    pgstacInfra.pgstacSecret.grantRead(eodcHubRole);

    eodcHubRole.addToPolicy(
      new cdk.aws_iam.PolicyStatement({
        effect: cdk.aws_iam.Effect.ALLOW,
        actions: ['cloudformation:*'],
        resources: [pgstacStackArn],
      })
    );

    eodcHubRole.addToPolicy(
      new cdk.aws_iam.PolicyStatement({
        effect: cdk.aws_iam.Effect.ALLOW,
        actions: ['s3:*'],
        resources: ['*'],
      })
    );

    const describeSecurityGroupsStatement = new cdk.aws_iam.PolicyStatement({
      actions: ['ec2:DescribeSecurityGroups'],
      resources: ['*'],
    });
    eodcHubRole.addToPolicy(describeSecurityGroupsStatement);
    const securityGroupStatement = new cdk.aws_iam.PolicyStatement({
      actions: ['ec2:ModifySecurityGroup*', 'ec2:AuthorizeSecurityGroupIngress'],
      resources: [`arn:aws:ec2:${this.region}:${this.account}:security-group/${securityGroupId}`],
    });
    eodcHubRole.addToPolicy(securityGroupStatement);
  }
}
const eodcHubRoleStack = new eodcHubRole(app, buildStackName('eodcHubRole'), {});
eodcHubRoleStack.addDependency(pgstacInfra);
