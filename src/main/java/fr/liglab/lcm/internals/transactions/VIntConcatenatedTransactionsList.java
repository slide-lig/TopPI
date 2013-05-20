package fr.liglab.lcm.internals.transactions;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.omg.CORBA.IntHolder;

public class VIntConcatenatedTransactionsList extends TransactionsList {

	public VIntConcatenatedTransactionsList(int nbTransactions, int[] distinctItemFreq) {
		int size = 0;
		for (int i = 0; i < distinctItemFreq.length; i++) {
			// add 1 because we use the value 0 for empty
			size += distinctItemFreq[i] * getVIntSize(i + 1);
		}
		// 2 ints for transaction end and support
		size += nbTransactions * 4 * 2;
		this.concatenated = new byte[size];
	}

	public VIntConcatenatedTransactionsList(int size) {
		this.concatenated = new byte[size];
	}

	private byte[] concatenated;

	static private int getVIntSize(int val) {
		if (val < 0) {
			return 5;
		} else if (val < 0x00000080) {
			return 1;
		} else if (val < 0x00004000) {
			return 2;
		} else if (val < 0x00200000) {
			return 3;
		} else if (val < 0x10000000) {
			return 4;
		} else {
			return 5;
		}
	}

	private int readVInt(IntHolder pos) {
		byte b = this.concatenated[pos.value];
		pos.value++;
		if (b >= 0) {
			return b;
		} else {
			int res = (b & 0x7F);
			int shift = 7;
			while (true) {
				b = this.concatenated[pos.value];
				pos.value++;
				if (b > 0) {
					res = res | (b << shift);
					break;
				} else {
					res = res | ((b & 0x7F) << shift);
					shift += 7;
				}
			}
			return res;
		}
	}

	private void writeVInt(IntHolder pos, int val) {
		while (true) {
			if (val >= 0 && val < 0x00000080) {
				this.concatenated[pos.value] = (byte) val;
				// System.out.println("encoding "
				// + String.format("%X", (byte) value));
				pos.value++;
				break;
			} else {
				// System.out.println("encoding "
				// + String.format("%X", ((byte) value)) + " into "
				// + String.format("%X", (((byte) value) | 0x80)));
				this.concatenated[pos.value] = (byte) (((byte) val) | 0x80);
				val = val >>> 7;
				pos.value++;
			}
		}
	}

	private void writeInt(IntHolder pos, int val) {
		writeInt(pos.value, val);
		pos.value += 4;
	}

	private int readInt(IntHolder pos) {
		int res = readInt(pos.value);
		pos.value += 4;
		return res;
	}

	private void writeInt(int pos, int val) {
		ByteBuffer bb = ByteBuffer.wrap(this.concatenated, pos, 4);
		bb.putInt(val);
		// TODO check
	}

	private int readInt(int pos) {
		// TODO check
		ByteBuffer bb = ByteBuffer.wrap(this.concatenated, pos, 4);
		return bb.getInt();
	}

	private void saveIntSpace(IntHolder pos) {
		pos.value += 4;
	}

	private void eraseLastVal(IntHolder pos) {
		// there is at least 1 byte in a vint, and it is positive
		int erase = pos.value - 1;
		concatenated[erase] = 0;
		erase--;
		// all other bytes of the vint are negative, stop when we see a positive
		for (erase = erase - 1; concatenated[erase] < 0; erase--) {
			concatenated[erase] = 0;
		}
	}

	@Override
	public Iterator<IterableTransaction> iterator() {
		return new TransIter();
	}

	@Override
	public TransactionIterator get(int transaction) {
		if (transaction >= this.concatenated.length) {
			throw new IllegalArgumentException("transaction " + transaction + " does not exist");
		}
		return new TransComp(transaction);
	}

	@Override
	public TransactionsWriter getWriter() {
		return new TransactionsWriter() {
			private IntHolder index = new IntHolder(0);
			private int lastTransactionId;

			@Override
			public void endTransaction() {
				writeInt(this.lastTransactionId, this.index.value);
			}

			@Override
			public int beginTransaction(int support) {
				this.lastTransactionId = this.index.value;
				saveIntSpace(this.index);
				writeInt(this.index, support);
				return this.lastTransactionId;
			}

			@Override
			public void addItem(int item) {
				// remember 0 is empty
				writeVInt(this.index, item + 1);
			}
		};
	}

	private final class TransComp extends TransactionIterator {
		private IntHolder pos;
		private final int startPos;
		private final int endPos;
		private int nextVal;

		public TransComp(int startPos) {
			this.startPos = startPos;
			this.pos = new IntHolder(startPos);
			this.endPos = readInt(this.pos);
			this.findNext();
		}

		@Override
		public int next() {
			int val = this.nextVal;
			this.findNext();
			return val;
		}

		private void findNext() {
			while (true) {
				if (pos.value == this.endPos) {
					this.nextVal = -1;
					return;
				} else {
					int val = readVInt(this.pos);
					if (val != 0) {
						// remove 1 because 0 is empty
						this.nextVal = val - 1;
						return;
					}
				}
			}
		}

		@Override
		public void remove() {
			eraseLastVal(this.pos);
		}

		@Override
		public boolean hasNext() {
			return this.nextVal != -1;
		}

		@Override
		public int getTransactionSupport() {
			return readInt(this.startPos + 4);
		}

		@Override
		public void setTransactionSupport(int s) {
			writeInt(this.startPos + 4, s);
		}
	}

	private final class TransIter implements Iterator<IterableTransaction> {
		private int support;
		private int pos;

		public TransIter() {
			if (concatenated.length < 8) {
				this.support = 0;
				return;
			} else {
				this.pos = 0;
				this.findNext();
			}
		}

		@Override
		public boolean hasNext() {
			return this.support > 0;
		}

		private void findNext() {
			while (true) {
				if (this.pos > concatenated.length) {
					this.support = 0;
					return;
				}
				this.support = readInt(this.pos + 4);
				if (this.support > 0) {
					return;
				} else {
					this.pos = readInt(this.pos);
				}
			}
		}

		@Override
		public IterableTransaction next() {
			IterableTrans res = new IterableTrans(this.pos);
			this.findNext();
			return res;
		}

		@Override
		public void remove() {
			concatenated[this.pos + 1] = 0;
		}

	}

	private final class IterableTrans implements IterableTransaction {
		private final int pos;

		public IterableTrans(final int pos) {
			this.pos = pos;
		}

		@Override
		public TransactionIterator iterator() {
			return new TransComp(this.pos);
		}

	}
}
