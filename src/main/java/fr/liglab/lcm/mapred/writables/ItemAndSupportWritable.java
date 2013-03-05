package fr.liglab.lcm.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class ItemAndSupportWritable implements Writable {
	
	private IntWritable item;
	private LongWritable support;
	
	public ItemAndSupportWritable() {
		item = new IntWritable();
		support = new LongWritable();
	}
	
	public ItemAndSupportWritable(int i, long s) {
		item = new IntWritable(i);
		support = new LongWritable(s);
	}
	
	public int getItem() {
		return item.get();
	}
	
	public long getSupport() {
		return support.get();
	}
	
	public void set(int i, long s) {
		item.set(i);
		support.set(s);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		item.readFields(in);
		support.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		item.write(out);
		support.write(out);
	}
	
	@Override
	public int hashCode() {
		return item.hashCode() + support.hashCode();
	}
	
	@Override
	public String toString() {
		return item.toString() + "\t" + support.toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ItemAndSupportWritable) {
			ItemAndSupportWritable o = (ItemAndSupportWritable) obj;
			return o.item.equals(item) && o.support.equals(support);
		}
		return false;
	}
}
