#!/bin/bash
cd bin
rm ../MapReduceComplete.jar
jar cvfm ../MapReduceComplete.jar ../sManifest.txt *
aws s3 sync ../codes s3://emr-cc/emr-code-complete --delete
aws s3 cp ../MapReduceComplete.jar s3://emr-cc/emr-code-complete/MapReduceComplete.jar
#aws s3 sync ../input s3://emr-cc/emr-input-pagerank --delete
rm -r ../output/complete/*
aws s3 sync ../output/complete s3://emr-cc/emr-output/complete --delete
rm -r ../output2/complete/*
aws s3 sync ../output2/complete s3://emr-cc/emr-output2/complete --delete
aws emr create-cluster --release-label emr-4.0.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=1,InstanceType=m3.2xlarge --steps Type=CUSTOM_JAR,Name="Custom JAR Step",ActionOnFailure=CONTINUE,Jar=s3://emr-cc/emr-code-complete/MapReduceComplete.jar,Args=["s3://emr-cc/emr-input-pagerank","s3://emr-cc/emr-output/complete/results","s3://emr-cc/emr-output2/complete/results"] --auto-terminate --log-uri s3://emr-cc/emr-log/complete --enable-debugging
open https://eu-west-1.console.aws.amazon.com/elasticmapreduce/home?region=eu-west-1#cluster-list:
