package hadoop.reducers;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map.Entry;

import hadoop.utils.Pair;
import hadoop.utils.Stripe;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class HybridReducer extends Reducer<Pair, IntWritable, Text, Stripe>{

	private Stripe stripeTableH = new Stripe();
	private int marginal = 0;
	private String currentTerm = null;
	
	private void emitResult(Context context) throws IOException, InterruptedException {
		for(Entry<Writable, Writable> entry : stripeTableH.entrySet()) {
			DoubleWritable s = (DoubleWritable) entry.getValue();
			
			double average = (double) s.get() / marginal;
			
			average = Double.parseDouble(new DecimalFormat("##.###").format(average));
  
			entry.setValue(new DoubleWritable(average));
		}
		
		context.write(new Text(currentTerm), stripeTableH);
	}
	
	@Override
	protected void reduce(Pair p, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
		if(currentTerm == null) {
			currentTerm = p.getKey();
		} else if(!currentTerm.equals(p.getKey())) {
			emitResult(context);
			
			//Reset
			stripeTableH.clear();
			marginal = 0;
			currentTerm = p.getKey();
		}
		
		Text key = new Text(p.getKey());
		for(IntWritable count : counts) {
			DoubleWritable vf = (DoubleWritable) stripeTableH.get(key);
			
			if(vf == null) {
				vf = new DoubleWritable(0);
			}
			
			stripeTableH.put(new Text(p.getValue()), new DoubleWritable(vf.get() + (double)count.get()));
			marginal = marginal + count.get();
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		emitResult(context);
	}
}
