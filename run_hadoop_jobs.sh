#!/bin/bash

read -p "Do you want to run {pair, stripe, hybrid} job? " jobName

# Input jobName={hybrid, pair, stripe}
#jobName=$1

#Get current folder
curent_dir=$(pwd)

# /home/cloudera/Desktop/Batch/Jars/HybridDriver.jar
jarPath=
classPath=
nameJar=

if [ $jobName == hybrid ]
then
	nameJar=HybridDriver.jar
	jarPath=$curent_dir/Jars/$nameJar
	classPath=$curent_dir/CrystalBall/bin/hybrid.MF
elif [ $jobName == pair ]
then
	nameJar=PairDriver.jar
	jarPath=$curent_dir/Jars/$nameJar
	classPath=$curent_dir/CrystalBall/bin/pair.MF
elif [ $jobName == stripe ]
then
	nameJar=StripeDriver.jar
	jarPath=$curent_dir/Jars/$nameJar
	classPath=$curent_dir/CrystalBall/bin/stripe.MF
else
	echo "Does not exist jobName " $jobName
fi

# classDriver={hybriddriver, pairdriver, stripedriver}
classDriver=$jobName"driver"

#HadoopProject
inputFilePath=$curent_dir/Input/Customer

#Hadoop
envPath=/user/cloudera
inputPath=$envPath/input
outputPath=$envPath/output/$jobName
 
echo "Step 0: Create Jar filename"

if [ ! -f $jarPath ]
then
	echo "Create new jar driver file"
	cd $curent_dir/CrystalBall/bin
	jar -cvfm $nameJar $jobName.MF *
	mv -f $nameJar $curent_dir/Jars
	cd $curent_dir
fi

echo "Step 1: Remove folder of input and output"
hadoop fs -rm -r $inputPath
hadoop fs -rm -r $outputPath

echo "Step 2: Create folder to store input data"
hadoop fs -mkdir $envPath $inputPath 

echo "Step 3: Copy data to input folder"
hadoop fs -put $inputFilePath $inputPath

echo "Step 4: Run Hadoop"
hadoop jar $jarPath $inputPath $outputPath

echo "Step 5: Copy from Hadoop to results local"
if [ -f $curent_dir/Outputs/$jobName ]
then
	rm -r $curent_dir/Outputs/$jobName
fi

hadoop fs -copyToLocal $outputPath $curent_dir/Outputs

echo "Step 6: View all results"
hadoop fs -cat $outputPath/*

read -p "Completed!"

