package fr.liglab.lcm.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Meant to be a value with IntWritable (item ID) as key
 */
public class GIDandRebaseWritable implements Writable {
	
	private IntWritable gid;
	private IntWritable rebased;
	
	public GIDandRebaseWritable() {
		gid = new IntWritable();
		rebased = new IntWritable();
	}
	
	public GIDandRebaseWritable(final int g, final int r) {
		gid = new IntWritable(g);
		rebased = new IntWritable(r);
	}
	
	public int getGid() {
		return gid.get();
	}
	
	public int getRebased() {
		return rebased.get();
	}
	
	public void set(final int g, final int r) {
		gid.set(g);
		rebased.set(r);
	}
	
	public void readFields(DataInput in) throws IOException {
		gid.readFields(in);
		rebased.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException {
		gid.write(out);
		rebased.write(out);
	}
	
	@Override
	public int hashCode() {
		return gid.hashCode() + rebased.hashCode();
	}
	
	@Override
	public String toString() {
		return "(" + rebased.toString() + ")\t" + gid.toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof GIDandRebaseWritable) {
			GIDandRebaseWritable o = (GIDandRebaseWritable) obj;
			return o.gid.equals(gid) && o.rebased.equals(rebased);
		}
		return false;
	}
}
