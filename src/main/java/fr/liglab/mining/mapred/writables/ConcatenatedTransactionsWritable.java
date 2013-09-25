package fr.liglab.mining.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;


public final class ConcatenatedTransactionsWritable implements Writable {
	
	private int toBeWrittenConcatenatedLengths;
	private List<int[]> toBeWritten;
	private int[] fromDisk;
	
	public ConcatenatedTransactionsWritable() {}
	
	/**
	 * @param buffer a list of transactions to be written to disk
	 * @param lengths cumulated length of the provided transactions
	 */
	public void set(List<int[]> buffer, int lengths) {
		this.toBeWritten = buffer;
		this.toBeWrittenConcatenatedLengths = lengths;
	}
	
	public int[] get() {
		return this.fromDisk;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		this.fromDisk = new int[length];
		
		for (int i = 0; i < length; i++) {
			this.fromDisk[i] = in.readInt();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.toBeWrittenConcatenatedLengths + this.toBeWritten.size());
		
		Iterator<int[]> it = this.toBeWritten.iterator();
		while (it.hasNext()) {
			final int[] transaction = it.next();
			out.writeInt(transaction.length);
			
			for (int item : transaction) {
				out.writeInt(item);
			}
		}
	}

}