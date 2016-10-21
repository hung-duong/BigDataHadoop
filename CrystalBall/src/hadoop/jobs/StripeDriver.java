package hadoop.jobs;

import hadoop.mappers.StripeMapper;
import hadoop.partitioners.StripePartitioner;
import hadoop.reducers.StripeReducer;
import hadoop.utils.Stripe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StripeDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new StripeDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
		Job job = new Job(conf, "StripeDriver");
		job.setJarByClass(StripeDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Stripe.class);
	    
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Stripe.class);
		
		job.setMapperClass(StripeMapper.class);
		job.setPartitionerClass(StripePartitioner.class);
		job.setReducerClass(StripeReducer.class);
				    
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	     
		job.setNumReduceTasks(3);
		
	    return job.waitForCompletion(true) ? 0 : 1;
	}
}
