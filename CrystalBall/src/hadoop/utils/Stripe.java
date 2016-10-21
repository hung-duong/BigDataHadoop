package hadoop.utils;

import java.util.Iterator;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Stripe extends MapWritable{

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder("{");
		
		Iterator<Writable> it = keySet().iterator();
		while(it.hasNext()) {
			Text key = (Text) it.next();
			String value = get(key).toString();
			
			strBuilder.append("(").append(key.toString()).append(", ").append(value).append("), ");
		}
		
		strBuilder.replace(strBuilder.length() - 2, strBuilder.length(), "");
		strBuilder.append("}");

		return strBuilder.toString();
	}

}
