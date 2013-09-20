package fr.liglab.mining.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Super-generic writer for a pair of integers (a,b)
 * It's safer to create sub-classes with semantically-named getter/setters/comparator factories
 * @author kirchgem
 */
public class ItemAndSupportWritable implements Writable {
	
	protected int item = 0;
	protected int support = 0;
	
	public ItemAndSupportWritable() {}
	
	public void set(int i, int s) {
		this.item = i;
		this.support = s;
	}
	
	public int getItem() {
		return item;
	}
	
	public int getSupport() {
		return support;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.item = in.readInt();
		this.support = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.item);
		out.writeInt(this.support);
	}
	
	@Override
	public int hashCode() {
		return this.item + this.support;
	}
	
	@Override
	public String toString() {
		return this.item + " (" + this.support + ")";
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ItemAndSupportWritable) {
			ItemAndSupportWritable o = (ItemAndSupportWritable) obj;
			return o.item == this.item && o.support == this.support;
		}
		return false;
	}
}
