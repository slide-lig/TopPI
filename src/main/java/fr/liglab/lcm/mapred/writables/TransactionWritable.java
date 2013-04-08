package fr.liglab.lcm.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import fr.liglab.lcm.internals.ConcatenatedDataset;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.mapred.Driver;

public class TransactionWritable implements Writable {

	protected int[] transaction;

	public TransactionWritable() {
		super();
		transaction = new int[0];
	}

	public TransactionWritable(int[] t) {
		super();
		transaction = t;
	}

	public int[] get() {
		return transaction;
	}

	public int getLength() {
		return transaction.length;
	}

	public void set(int[] t) {
		transaction = t;
	}

	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		transaction = new int[length];

		for (int i = 0; i < transaction.length; i++) {
			transaction[i] = in.readInt();
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(transaction.length);
		for (int i = 0; i < transaction.length; i++) {
			out.writeInt(transaction[i]);
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		String space = "";

		for (int i = 0; i < transaction.length; i++) {
			builder.append(space);
			builder.append(transaction[i]);
			space = " ";
		}

		return builder.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TransactionWritable) {
			TransactionWritable other = (TransactionWritable) obj;
			if (other.transaction.length == transaction.length) {
				for (int i = 0; i < transaction.length; i++) {
					if (transaction[i] != other.transaction[i])
						return false;
				}
				return true;
			}
		}

		return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(this.transaction);
	}
	
	
	public static final Dataset buildDataset(Configuration conf, Iterator<TransactionWritable> transactions) {
		final WritableTransactionsIterator input = new WritableTransactionsIterator(transactions);
		int minSupport = conf.getInt(Driver.KEY_MINSUP, 10);
		
		ConcatenatedDataset dataset = null;
		try {
			dataset = new ConcatenatedDataset(minSupport, input);
		} catch (DontExploreThisBranchException e) {
			// with initial support coreItem = MAX_ITEM , this won't happen
			e.printStackTrace();
		}
		
		return dataset;
	}
	
	public static final class WritableTransactionsIterator implements Iterator<int[]> {
		
		private final Iterator<TransactionWritable> wrapped;
		
		public WritableTransactionsIterator(Iterator<TransactionWritable> original) {
			this.wrapped = original;
		}
		
		public boolean hasNext() {
			return this.wrapped.hasNext();
		}

		public int[] next() {
			return this.wrapped.next().get();
		}

		public void remove() {
			throw new NotImplementedException();
		}
	}
}
