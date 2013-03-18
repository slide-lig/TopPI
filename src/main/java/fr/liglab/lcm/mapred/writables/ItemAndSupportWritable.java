package fr.liglab.lcm.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

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
	
	
	
	
	public static class SortComparator extends WritableComparator {
		
		public SortComparator() {
			super(ItemAndSupportWritable.class);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable one, WritableComparable other) {
			if (one instanceof ItemAndSupportWritable && other instanceof ItemAndSupportWritable) {
				ItemAndSupportWritable is1 = (ItemAndSupportWritable) one;
				ItemAndSupportWritable is2 = (ItemAndSupportWritable) other;
				return is1.compareTo(is2);
			} else {
				return super.compare(one, other);
			}
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int item1 = readInt(b1, s1);
			int item2 = readInt(b2, s2);
			
			if (item1 == item2) {
				int support1 = readInt(b1, s1 + Integer.SIZE / Byte.SIZE);
				int support2 = readInt(b2, s2 + Integer.SIZE / Byte.SIZE);
				
				return support2 - support1;
			} else {
				return item1 - item2;
			}
		}
	}
	
	
	public static class ItemOnlyComparator extends WritableComparator {
		
		public ItemOnlyComparator() {
			super(ItemAndSupportWritable.class);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable one, WritableComparable other) {
			if (one instanceof ItemAndSupportWritable && other instanceof ItemAndSupportWritable) {
				ItemAndSupportWritable is1 = (ItemAndSupportWritable) one;
				ItemAndSupportWritable is2 = (ItemAndSupportWritable) other;
				return is1.getItem() - is2.getItem();
			} else {
				return super.compare(one, other);
			}
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int item1 = readInt(b1, s1);
			int item2 = readInt(b2, s2);
			return item1 - item2;
		}
	}
}
