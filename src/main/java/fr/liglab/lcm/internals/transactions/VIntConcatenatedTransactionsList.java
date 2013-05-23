package fr.liglab.lcm.internals.transactions;

import java.util.Arrays;
import java.util.Iterator;

import org.omg.CORBA.IntHolder;

import fr.liglab.lcm.internals.nomaps.Counters;

public class VIntConcatenatedTransactionsList extends TransactionsList {
	public static boolean compatible(Counters c) {
		return c.getMaxFrequent() < Integer.MAX_VALUE;
	}

	public static int getMaxTransId(Counters c) {
		long l = c.distinctTransactionLengthSum * 5 + c.distinctTransactionsCount * 4 - 1;
		if (l > Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		} else {
			return (int) l;
		}
	}

	private byte[] concatenated;
	private int size;

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

	@Override
	public TransactionsList clone() {
		VIntConcatenatedTransactionsList o = (VIntConcatenatedTransactionsList) super.clone();
		o.concatenated = Arrays.copyOf(this.concatenated, this.concatenated.length);
		return o;
	}

	@Override
	public int size() {
		return this.size;
	}

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

	@SuppressWarnings("unused")
	private int readInt(IntHolder pos) {
		int res = readInt(pos.value);
		pos.value += 4;
		return res;
	}

	private void writeInt(int pos, int val) {
		// System.out.println("encoding " + String.format("%X", val) + " " +
		// val);
		concatenated[pos] = (byte) (val >> 24);
		concatenated[pos + 1] = (byte) (val >> 16);
		concatenated[pos + 2] = (byte) (val >> 8);
		concatenated[pos + 3] = (byte) val;
	}

	private int readInt(int pos) {
		return concatenated[pos] << 24 | (concatenated[pos + 1] & 0xFF) << 16 | (concatenated[pos + 2] & 0xFF) << 8
				| (concatenated[pos + 3] & 0xFF);
	}

	private void saveIntSpace(IntHolder pos) {
		pos.value += 4;
	}

	private void eraseLastVal(int pos) {
		// there is at least 1 byte in a vint, and it is positive
		int erase = pos - 1;
		concatenated[erase] = 0;
		// all other bytes of the vint are negative, stop when we see a positive
		for (erase--; concatenated[erase] < 0; erase--) {
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
				size++;
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
		private int erasePos;
		private final int startPos;
		private final int endPos;
		private int nextVal;

		public TransComp(int startPos) {
			this.startPos = startPos;
			this.endPos = readInt(startPos);
			this.pos = new IntHolder(startPos + 8);
			this.findNext();
		}

		@Override
		public int next() {
			int val = this.nextVal;
			this.erasePos = this.pos.value;
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
			eraseLastVal(erasePos);
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
			if (s <= 0) {
				size--;
			}
			writeInt(this.startPos + 4, s);
		}
	}

	private final class TransIter implements Iterator<IterableTransaction> {
		private int pos;

		public TransIter() {
			if (concatenated.length < 8) {
				this.pos = -1;
				return;
			} else {
				this.pos = 0;
				this.findNext(true);
			}
		}

		@Override
		public boolean hasNext() {
			return this.pos >= 0;
		}

		private void findNext(boolean firstTime) {
			while (true) {
				if (firstTime) {
					firstTime = false;
				} else {
					this.pos = readInt(this.pos);
				}
				if (this.pos >= concatenated.length) {
					this.pos = -1;
					return;
				}
				int support = readInt(this.pos + 4);
				if (support != 0) {
					return;
				}
			}
		}

		@Override
		public IterableTransaction next() {
			IterableTrans res = new IterableTrans(this.pos);
			this.findNext(false);
			return res;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
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
