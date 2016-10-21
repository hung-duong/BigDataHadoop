package hadoop.jobs;

import hadoop.mappers.HybridMapper;
import hadoop.reducers.HybridReducer;
import hadoop.utils.Pair;
import hadoop.utils.Stripe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HybridDriver extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HybridDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
		Job job = new Job(conf, "HybridDriver");
		job.setJarByClass(HybridDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(HybridMapper.class);
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
	    job.setReducerClass(HybridReducer.class); 
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Stripe.class);
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	}
}
