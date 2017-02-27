#!/bin/bash
cd bin
rm ../MapReduceSimplified.jar
jar cvfm ../MapReduceSimplified.jar ../pManifest.txt *
rm -r ../localoutput/simplified/*
rm -r ../localoutput2/simplified/*
cd ..
java -cp hadoop26lib/*:MapReduceSimplified.jar simplified.MapReduce input	localoutput/simplified/results localoutput2/simplified/results
