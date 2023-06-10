#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";

import { Vpc } from "./vpc";
import { Config } from "./config";
import { PgStacInfra } from "./pgstacinfra";

const { terminationProtection, stage, version, buildStackName, tags } =
  new Config();

export const app = new cdk.App({});

const { vpc } = new Vpc(app, buildStackName("vpc"), {
  terminationProtection,
  tags,
  natGatewayCount: stage === "prod" ? undefined : 1,
});

new PgStacInfra(app, buildStackName("pgSTAC"), {
  vpc,
  terminationProtection,
  tags,
  stage,
  version,
});
