package hadoop.reducers;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Map.Entry;

import hadoop.utils.Stripe;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeReducer extends Reducer<Text, Stripe, Text, Stripe> {

	@Override
	protected void reduce(Text key, Iterable<Stripe> tfs, Context context) throws IOException, InterruptedException {
		
		int marginal = 0;
		
		Stripe stripeHf = new Stripe();
		
		for(Stripe stripeH : tfs) {
			Iterator<Entry<Writable, Writable>> it = stripeH.entrySet().iterator();
			
			while(it.hasNext()) {
				Entry<Writable, Writable> e = it.next();
				
				Text k = (Text) e.getKey();
				IntWritable v = (IntWritable) e.getValue();
				
				marginal += v.get();
				
				DoubleWritable vf = (DoubleWritable) stripeHf.get(k);
				if(vf == null) {
					vf = new DoubleWritable(0);
				}
				
				stripeHf.put(k, new DoubleWritable((double)v.get() + vf.get()));
			}
		}
		
		for(Entry<Writable, Writable> entry : stripeHf.entrySet()) {
			DoubleWritable s = (DoubleWritable) entry.getValue();
			
			double average = (double) s.get() / marginal;
			
			average = Double.parseDouble(new DecimalFormat("##.###").format(average));
  
			entry.setValue(new DoubleWritable(average));
		}
		
		context.write(key, stripeHf);
	}
}
