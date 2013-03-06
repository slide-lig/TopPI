package fr.liglab.lcm.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ItemAndSupportWritable implements WritableComparable<ItemAndSupportWritable> {
	
	private int item;
	private int support;
	
	public ItemAndSupportWritable() {}
	
	public ItemAndSupportWritable(int i, int s) {
		item = i;
		support = s;
	}
	
	public void setItem(int i) {
		item = i;
	}
	
	public int getItem() {
		return item;
	}
	
	public void setSupport(int s) {
		support = s;
	}
	
	public int getSupport() {
		return support;
	}
	
	public void set(int i, int s) {
		item = i;
		support = s;
	}
	
	public void readFields(DataInput in) throws IOException {
		item = in.readInt();
		support = in.readInt();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(item);
		out.writeInt(support);
	}
	
	@Override
	public int hashCode() {
		return super.hashCode() + item + support;
	}
	
	@Override
	public String toString() {
		return item + " (" + support + ")";
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ItemAndSupportWritable) {
			ItemAndSupportWritable o = (ItemAndSupportWritable) obj;
			return o.item == this.item && o.support == this.support;
		}
		return false;
	}

	public int compareTo(ItemAndSupportWritable other) {
		if (other.item == this.item) {
			return other.support - this.support;
		} else {
			return this.item - other.item;
		}
	}
}
