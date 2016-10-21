package hadoop.partitioners;

import java.util.Arrays;
import hadoop.utils.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class PairPartitioner extends Partitioner<Pair, IntWritable> {

	@Override
	public int getPartition(Pair key, IntWritable count, int numReduceTasks) {
		String[] part1 = {"12", "34"};
		String[] part2 = {"56"};
		
		if(numReduceTasks == 0) {
			return 0;
		}
		
		if (Arrays.asList(part1).contains(key.getKey())) {
			return 0;
		} else if (Arrays.asList(part2).contains(key.getKey())) {
			return 1 % numReduceTasks;
		} else {
			return 2 % numReduceTasks;
		}
	}

}
