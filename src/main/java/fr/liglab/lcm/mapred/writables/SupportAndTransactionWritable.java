package fr.liglab.lcm.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class SupportAndTransactionWritable implements Writable {
	private LongWritable support;
	private TransactionWritable transaction;
	
	public SupportAndTransactionWritable() {
		super();
		support = new LongWritable();
		transaction = new TransactionWritable();
	}
	
	public SupportAndTransactionWritable(long supp, int[] tran) {
		super();
		support = new LongWritable(supp);
		transaction = new TransactionWritable(tran);
	}
	
	public void set(long supp, int[] tran) {
		support.set(supp);
		transaction.set(tran);
	}
	
	public long getSupport() {
		return support.get();
	}
	
	public int[] getTransaction() {
		return transaction.get();
	}
	
	public void write(DataOutput out) throws IOException {
		support.write(out);
		transaction.write(out);
	}
	
	public void readFields(DataInput in) throws IOException {
		support.readFields(in);
		transaction.readFields(in);
	}	
	
	@Override
	public int hashCode() {
		return support.hashCode() + transaction.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof SupportAndTransactionWritable) {
			SupportAndTransactionWritable other = (SupportAndTransactionWritable) o;
			return support.equals(other.support) && 
					transaction.equals(other.transaction);
		}
		return false;
	}
	
	@Override
	public String toString(){
		return support.toString() + "\t" + transaction.toString();
	}
}
