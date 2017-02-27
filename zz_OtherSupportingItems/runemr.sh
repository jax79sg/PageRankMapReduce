#!/bin/bash
cd bin
rm ../MapReduceSimplified.jar
jar cvfm ../MapReduceSimplified.jar ../pManifest.txt *
aws s3 sync ../codes s3://emr-cc/emr-code-simplified --delete
aws s3 cp ../MapReduceSimplified.jar s3://emr-cc/emr-code-simplified/MapReduceSimplified.jar
#aws s3 sync ../input s3://emr-cc/emr-input-pagerank --delete
rm -r ../output/simplified/*
aws s3 sync ../output/simplified s3://emr-cc/emr-output/simplified --delete
rm -r ../output2/simplified/*
aws s3 sync ../output2/simplified s3://emr-cc/emr-output2/simplified --delete
aws emr create-cluster --release-label emr-4.0.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=1,InstanceType=m3.2xlarge --steps Type=CUSTOM_JAR,Name="Custom JAR Step",ActionOnFailure=CONTINUE,Jar=s3://emr-cc/emr-code-simplified/MapReduceSimplified.jar,Args=["s3://emr-cc/emr-input-pagerank","s3://emr-cc/emr-output/simplified/results","s3://emr-cc/emr-output2/simplified/results"] --auto-terminate --log-uri s3://emr-cc/emr-log/simplified --enable-debugging
open https://eu-west-1.console.aws.amazon.com/elasticmapreduce/home?region=eu-west-1#cluster-list:
