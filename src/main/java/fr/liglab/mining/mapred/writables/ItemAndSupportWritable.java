package fr.liglab.mining.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Super-generic writer for a pair of integers (a,b)
 * It's safer to create sub-classes with semantically-named getter/setters/comparator factories
 * @author kirchgem
 */
public class ItemAndSupportWritable implements WritableComparable<ItemAndSupportWritable> {
	
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
