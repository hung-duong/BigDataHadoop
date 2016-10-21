package hadoop.partitioners;

import java.util.Arrays;

import hadoop.utils.Stripe;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StripePartitioner extends Partitioner<Text, Stripe> {

	@Override
	public int getPartition(Text key, Stripe st, int numReduceTasks) {
		String[] part1 = {"12", "34"};
		String[] part2 = {"56"};
		
		if(numReduceTasks == 0) {
			return 0;
		}
		
		if (Arrays.asList(part1).contains(key.toString())) {
			return 0;
		} else if (Arrays.asList(part2).contains(key.toString())) {
			return 1 % numReduceTasks;
		} else {
			return 2 % numReduceTasks;
		}
	}
}
