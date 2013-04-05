package fr.liglab.lcm.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

public class VIntTransactionWritable extends TransactionWritable {

	@Override
	public void readFields(DataInput in) throws IOException {
		int length = WritableUtils.readVInt(in);
		transaction = new int[length];
		for (int i = 0; i < transaction.length; i++) {
			transaction[i] = WritableUtils.readVInt(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, transaction.length);
		for (int i = 0; i < transaction.length; i++) {
			WritableUtils.writeVInt(out, transaction[i]);
		}
	}

}
