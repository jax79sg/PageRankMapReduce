#!/bin/bash
cd bin
rm ../MapReduceComplete.jar
jar cvfm ../MapReduceComplete.jar ../sManifest.txt *
rm -r ../localoutput/complete/*
rm -r ../localoutput2/complete/*
cd ..
java -cp hadoop26lib/*:MapReduceComplete.jar complete.MapReduce input localoutput/complete/results localoutput2/complete/results
