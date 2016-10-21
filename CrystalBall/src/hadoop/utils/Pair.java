package hadoop.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class Pair implements WritableComparable<Pair> {
	private String key;
	private String value;
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	public Pair() {				
		this.key = "";
		this.value = "";
	}
	
	public Pair(String k, String v) {
		this.key = k;
		this.value = v;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (in == null) {
			throw new IllegalArgumentException("Input cannot be NULL.");
		}
		this.key = in.readUTF();
		this.value = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (out == null) {
			throw new IllegalArgumentException("Output cannot be NULL.");
		}
		out.writeUTF(this.key);
		out.writeUTF(this.value);
	}
	
	@Override
	public int compareTo(Pair o) {
		if (o == null || !(o instanceof Pair))
			return -1;
		
		int cmp = this.key.compareTo(o.key);
		if (cmp != 0) {
			return cmp;
		}
		
		return this.value.compareTo(o.value);
	}

	@Override
	public int hashCode() {
		int hash = 31;
		hash = hash * 17 + this.key.hashCode();
		hash = hash * 17 + this.value.hashCode();

		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Pair))
			return false;

		Pair p = (Pair) obj;
		return this.key.equals(p.getKey()) && this.value.equals(p.getValue());
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		
		return str.append("(").append(this.key).append(", ").append(this.value).append(")").toString();
		
	}
	
	
}
