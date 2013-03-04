package fr.liglab.lcm.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Meant to be a value with IntWritable (item ID) as key
 */
public class GIDandRebaseWritable implements Writable {
	
	private IntWritable gid;
	private Text rebased;
	
	public GIDandRebaseWritable() {
		gid = new IntWritable();
		rebased = new Text();
	}
	
	public GIDandRebaseWritable(int i, String s) {
		gid = new IntWritable(i);
		rebased = new Text(s);
	}
	
	public int getGid() {
		return gid.get();
	}
	
	public String getRebased() {
		return rebased.toString();
	}
	
	public void set(int i, String s) {
		gid.set(i);
		rebased.set(s);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		gid.readFields(in);
		rebased.readFields(in);
	}

	@Override
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
