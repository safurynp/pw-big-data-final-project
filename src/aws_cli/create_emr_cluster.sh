#!/bin/bash

aws emr create-cluster \
 --name "Cluster for Analysis" \
 --log-uri "s3n://aws-logs-363710412655-us-east-1/elasticmapreduce/" \
 --release-label "emr-7.0.0" \
 --service-role "arn:aws:iam::363710412655:role/EMR_DefaultRole" \
 --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","EmrManagedMasterSecurityGroup":"sg-0c5a36b6b9fdc95ff","EmrManagedSlaveSecurityGroup":"sg-0a2a853c561ac8f84","KeyName":"vockey","AdditionalMasterSecurityGroups":[],"AdditionalSlaveSecurityGroups":[],"SubnetId":"subnet-04d8ad13bdac04b26"}' \
 --applications Name=Hadoop Name=Hive Name=JupyterEnterpriseGateway Name=Livy Name=Spark \
 --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","Name":"Primary","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}},{"InstanceCount":1,"InstanceGroupType":"CORE","Name":"Core","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}},{"InstanceCount":1,"InstanceGroupType":"TASK","Name":"Task - 1","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}}]' \
 --scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
 --auto-termination-policy '{"IdleTimeout":3600}' \
 --region "us-east-1"