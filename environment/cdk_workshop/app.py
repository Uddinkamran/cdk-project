#!/usr/bin/env python3
import os
import aws_cdk as cdk

from aws_cdk import aws_s3, aws_kinesis, aws_glue, aws_s3_assets, aws_iam

from cdk_workshop.cdk_workshop_stack import CdkWorkshopStack


app = cdk.App()
CdkWorkshopStack(app, "cdk-workshop")

# S3
class S3Resource(cdk.Stack):
    def __init__(self, scope, construct_id, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.data_bucket = aws_s3.Bucket(
            self,
            id = "glueData",
            bucket_name = "glueprocesseddata"
            )
# Kinesis
class KinesisResource(cdk.Stack):
    def __init__(self, scope, construct_id, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.data_pipe_stream = aws_kinesis.Stream(
            self,
            "dataStream",
            retention_period = cdk.Duration.days(5),
            shard_count = 1,
            stream_name = "data-pipe-stream"
            )

# Glue
class GlueResource(cdk.Stack):
    def __init__(self, scope, construct_id, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.glue_db_cfn_param = cdk.CfnParameter(
            self,
            "glueDbName",
            default = "glue_test_db"
            )
            
        self.glue_table_cfn_param = cdk.CfnParameter(
            self,
            "glueTableName",
            default = "glue_test_table"
            )
            
        glue_db = aws_glue.CfnDatabase(
            self,
            "glueDb",
            catalog_id = cdk.Aws.ACCOUNT_ID,
            database_input = aws_glue.CfnDatabase.DatabaseInputProperty(
                name = self.glue_db_cfn_param.value_as_string
                )
            )
            
        # url- string
        # method - string
        
        glue_table = aws_glue.CfnTable(
            self,
            "glueTable",
            catalog_id = glue_db.catalog_id,
            database_name = self.glue_db_cfn_param.value_as_string,
            table_input = aws_glue.CfnTable.TableInputProperty(
                description = "example input table",
                name = self.glue_table_cfn_param.value_as_string,
                partition_keys = [
                    {"name": "url", "type": "string"}
                    
                    ],
                    storage_descriptor = aws_glue.CfnTable.StorageDescriptorProperty(
                        columns = [
                            {"name": "method", "type": "string"}
                        
                        ]
                    )
                )
            )
            
        glue_table.add_depends_on(glue_db)
        
class GlueJobResource(cdk.Stack):
    def __init__(self, scope, construct_id, input_stream, glue_db, glue_table, etl_bucket, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.etl_role = aws_iam.Role(
            self,
            "etlRole",
            assumed_by = aws_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies = [
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3ReadOnlyAccess"),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
                ]
        )
        
        self.etl_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions = [
                    "s3:*"
                ],
                resources = [
                    f"{etl_bucket.bucket_arn}",
                    f"{etl_bucket.bucket_arn}/*"
                ]
            
            )
            
        )
        
        self.etl_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions = [
                    "kinesis:DescribeStream"
                ],
                resources = [
                    f"{input_stream.stream_arn}"
                ]
            )
        )
        
        input_stream.grant_read(self.etl_role)
        
        etl_script_asset = aws_s3_assets.Asset(
            self,
            "etlScriptAsset",
            path = "scripts/Main.scala"
            
        )
        
        glue_etl_job = aws_glue.CfnJob(
            self,
            "datalakeInput",
            name = "stream-etl-process",
            role = self.etl_role.role_arn,
            command = aws_glue.CfnJob.JobCommandProperty(
                name = "gluestreaming",
                script_location = "s3://{bucket}/{key}".format(
                    bucket = etl_script_asset.s3_bucket_name,
                    key = etl_script_asset.s3_object_key
                )
            ),
            default_arguments = {
                "--src_db_name": glue_db,
                "--src_tbl_name": glue_table,
                "--datalake_bkt_name": etl_bucket.bucket_name,
                "--datalake_bkt_prefix": "myetl",
                "--job-language": "scala",
                "--class": "GlueApp",
                "--job-bookmark-option": "job-bookmark-enable"
            }

            
        )

        
        job_trigger = aws_glue.CfnTrigger(
            self,
            "etlJobTrigger",
            type = "SCHEDULED",
            schedule = "cron(0/10 * * * ? *)",
            start_on_creation = True,
            actions = [
                aws_glue.CfnTrigger.ActionProperty(
                    job_name = glue_etl_job.name
                )
            ]
        )
        
        job_trigger.add_depends_on(glue_etl_job)
        
PROJECT = "data-pipe-stream-with-aws"
                

s3 = S3Resource(
    app,
    "{stack}-s3".format(stack = PROJECT)
)
    
kinesis = KinesisResource(
    app,
    "{stack}-kinesis".format(stack = PROJECT)
)

glue_table = GlueResource(
    app,
    "{stack}-glue-table".format(stack = PROJECT)
)

glue_job = GlueJobResource(
    app,
    "{stack}-glue-job".format(stack = PROJECT),
    input_stream = kinesis.data_pipe_stream,
    glue_db = glue_table.glue_db_cfn_param.value_as_string,
    glue_table = glue_table.glue_db_cfn_param.value_as_string,
    etl_bucket = s3.data_bucket
    
)



app.synth()
