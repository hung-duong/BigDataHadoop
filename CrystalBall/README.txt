CrystalBall is a project to predict events that may happen once a certain event happened.

To resolve this project, we gave three ways based on the Hadoop theory.
1. Pair approach
2. Stripe approach
3. Hybrid approach

We deployed all of three approach using the in-mapper combining and partitioner(only for Pair and Stripe approach).
The partitioner will help diving to three Reducers based on the output mapper.

Structure of project
src
|----Hadoop
		|------jobs
		|		|--------HybridDriver.java
		|		|--------PairDriver.java
		|		|--------StripeDriver.java
		|
		|------mappers
		|		|--------HybridMapper.java
		|		|--------PairMapper.java
		|		|--------StripeMapper.java
		|
		|------reducers
		|		|--------HybridReducer.java
		|		|--------PairReducer.java
		|		|--------StripeReducer.java
		|
		|------partitioners
		|		|--------PairPartitioner.java
		|		|--------StripePartitioner.java
		|
		|------utils
				|--------Pair.java
				|--------Stripe.java
			
How to run by batch file
1. Copy the Hadoop project folder to local2. Cd to Hadoop project3. Double click on run_hadoop_job.sh and run it with terminal4. A message “Do you want to run {pair, stripe, hybrid} job?” display on terminal and type one of them {pair, stripe, hybrid}






