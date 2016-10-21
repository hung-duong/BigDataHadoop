package hadoop.mappers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import hadoop.utils.Pair;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HybridMapper extends Mapper<LongWritable, Text, Pair, IntWritable>{
	private Map<Pair, Integer> pairTable = new HashMap<>();
	
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
					Pair p = new Pair(strArr[i], strArr[j]);	
					if(!pairTable.containsKey(p)) {
						pairTable.put(p, 1);
					} else {
						int intRef = pairTable.get(p);
						pairTable.put(p, intRef + 1);
					}
					
					j++;
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Entry<Pair, Integer> entry : pairTable.entrySet()) {
			context.write(entry.getKey(), new IntWritable(entry.getValue()));
		}
	}

}
