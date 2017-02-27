#!/bin/bash
rm -r ./output/simplified/results
aws s3 sync s3://emr-cc/emr-output/simplified ./output/simplified
rm -r ./output2/simplified/results
aws s3 sync s3://emr-cc/emr-output2/simplified ./output2/simplified

rm -r ./output/complete/results
aws s3 sync s3://emr-cc/emr-output/complete ./output/complete
rm -r ./output2/complete/results
aws s3 sync s3://emr-cc/emr-output2/complete ./output2/complete



