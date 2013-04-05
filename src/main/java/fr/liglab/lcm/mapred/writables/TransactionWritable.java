package fr.liglab.lcm.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

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
}
