package hadoop.mappers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import hadoop.utils.Stripe;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripeMapper extends Mapper<LongWritable, Text, Text, Stripe> {

	private Map<String, Stripe> stripTable = new HashMap<>();
	 
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] strArr = value.toString().trim().split(" ");
		
		for(int i = 1; i < strArr.length; i++) {
			boolean stop = false;
			int j = i + 1;
			while(!stop && j < strArr.length) {
				if(strArr[j].equals(strArr[i])) {
					stop = true;
				} else {		
					Text neighbor = new Text(strArr[j]);
					Stripe stripeRef;
					
					if(!stripTable.containsKey(strArr[i])) {
						stripTable.put(strArr[i], new Stripe());
						
						stripeRef = stripTable.get(strArr[i]);
						stripeRef.put(neighbor, new IntWritable(1));
					} else {
						stripeRef = stripTable.get(strArr[i]);
						
						if(!stripeRef.containsKey(neighbor)) {
							stripeRef.put(neighbor, new IntWritable(1));
						} else {
							IntWritable num = (IntWritable) stripeRef.get(neighbor);
							num.set(num.get() + 1);
							stripeRef.put(neighbor, num);
						}
					}
					
					j++;
				}
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Entry<String, Stripe> entry : stripTable.entrySet()) {
			context.write(new Text(entry.getKey()), entry.getValue());
		}
	}
}
