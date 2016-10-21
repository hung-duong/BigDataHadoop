package hadoop.reducers;

import java.io.IOException;
import java.text.DecimalFormat;

import hadoop.utils.Pair;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairReducer extends Reducer<Pair, IntWritable, Pair, DoubleWritable>{

	private int margial = 0;

	@Override
	protected void reduce(Pair p, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
			
		if("*".equals(p.getValue())) {
			margial = 0;
			
			for(IntWritable count : counts) {
				margial += count.get();
			}
			
		} else {
			int sum = 0;
			
			for(IntWritable count : counts) {
				sum += count.get();
			}
			
			double average = (double) sum / margial;
			
			average = Double.parseDouble(new DecimalFormat("##.###").format(average));
			
			context.write(p, new DoubleWritable(average));
		}
	}

}
