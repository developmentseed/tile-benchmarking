import {
  Stack,
  StackProps,
  aws_ec2 as ec2,
  aws_rds as rds,
  aws_secretsmanager as secretsmanager,
  CfnOutput
} from "aws-cdk-lib";
import { Construct } from "constructs";
import {
  PgStacApiLambda,
  PgStacDatabase,
} from "cdk-pgstac";

export class PgStacInfra extends Stack {
  pgstacSecret: secretsmanager.ISecret;
  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id, props);

    const { vpc, stage, version } = props;

    const { db, pgstacSecret } = new PgStacDatabase(this, "pgstac-db", {
      vpc,
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.VER_14,
      }),
      vpcSubnets: {
        subnetType: ec2.SubnetType.PUBLIC
      },
      allocatedStorage: 1024,
      publiclyAccessible: true,
      pgstacVersion: '0.7.6',
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T4G, ec2.InstanceSize.SMALL)
    });
    this.pgstacSecret = pgstacSecret;
    const stackName = this.stackName;
    const stackArn = `arn:aws:cloudformation:${this.region}:${this.account}:stack/${stackName}/*`;
    new CfnOutput(this, 'pgstacinfra-stackArn', {
      value: stackArn,
      exportName: `${this.stackName}-stackArn`
    });
    new CfnOutput(this, 'pgstacinfra-securityGroupId', {
      value: this.resolve(db.connections.securityGroups[0].securityGroupId),
      exportName: `${this.stackName}-securityGroupId`
    });

    const apiSubnetSelection: ec2.SubnetSelection = {
      subnetType: props.dbSubnetPublic
        ? ec2.SubnetType.PUBLIC
        : ec2.SubnetType.PRIVATE_WITH_EGRESS,
    };

    const { url } = new PgStacApiLambda(this, "pgstac-api", {
      apiEnv: {
        NAME: `EODC STAC API (${stage})`,
        VERSION: version,
        DESCRIPTION: "STAC API for EODC Tile Benchmarking.",
      },
      vpc,
      db,
      dbSecret: pgstacSecret,
      subnetSelection: apiSubnetSelection,
    });

  }
}

export interface Props extends StackProps {
  vpc: ec2.Vpc;

  /**
   * Stage this stack. Used for naming resources.
   */
  stage: string;

  /**
   * Version of this stack. Used to correlate codebase versions
   * to services running.
   */
  version: string;

  /**
   * Flag to control whether database should be deployed into a
   * public subnet.
   */
  dbSubnetPublic?: boolean;
}
