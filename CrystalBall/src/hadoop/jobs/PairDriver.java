package hadoop.jobs;

import hadoop.mappers.PairMapper;
import hadoop.partitioners.PairPartitioner;
import hadoop.reducers.PairReducer;
import hadoop.utils.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PairDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PairDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
		Job job = new Job(conf, "PairDriver");
		job.setJarByClass(PairDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Pair.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(PairMapper.class);
		job.setPartitionerClass(PairPartitioner.class);
		job.setReducerClass(PairReducer.class);
		
		job.setNumReduceTasks(3);
	     
	    return job.waitForCompletion(true) ? 0 : 1;
	}
}
