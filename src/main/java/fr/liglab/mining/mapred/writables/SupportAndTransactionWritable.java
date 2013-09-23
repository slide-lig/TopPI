package fr.liglab.mining.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SupportAndTransactionWritable implements Writable {
	
	private int support;
	private int[] transaction;
	
	public void set(int s, int[] t) {
		this.support = s;
		this.transaction = t;
	}
	
	public int getSupport() {
		return support;
	}
	
	public int[] getTransaction() {
		return transaction;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(this.support);
		sb.append('\t');
		
		char sep = 0;
		
		for (int item : this.transaction) {
			if (sep == 0) {
				sep = ' ';
			} else {
				sb.append(sep);
			}
			
			sb.append(item);
		}
		
		return sb.toString(); 
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.support = in.readInt();
		
		final int length = in.readInt();
		this.transaction = new int[length];
		
		for (int i = 0; i < this.transaction.length; i++) {
			this.transaction[i] = in.readInt();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.support);
		out.writeInt(this.transaction.length);
		
		for (int item : this.transaction) {
			out.writeInt(item);
		}
	}
}
