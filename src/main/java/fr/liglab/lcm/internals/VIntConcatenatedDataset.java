package fr.liglab.lcm.internals;

import java.util.Iterator;

import org.omg.CORBA.IntHolder;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import fr.liglab.lcm.io.FileReader;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;

/**
 * Here all transactions are prefixed by their length and concatenated in a
 * single int[]
 * 
 * This dataset internally performs - basic reduction : transactions contain
 * only items having a support count in [minusp, 100% [ - occurrence delivery :
 * occurrences are stored as indexes in the concatenated array - fast
 * prefix-preserving test (see inner class CandidatesIterator)
 */
public class VIntConcatenatedDataset extends FilteredDataset {
	protected static int nbBytesForTransSize = 5;
	protected byte[] concatenated;

	public static void setMaxTransactionLength(int length) {
		int nbBits = ((int) Math.floor(Math.log(length) / (Math.log(2)))) + 1;
		nbBytesForTransSize = (int) Math.ceil(nbBits / 7.);
	}

	public VIntConcatenatedDataset(int minimumsupport,
			Iterator<int[]> transactions) throws DontExploreThisBranchException {
		super(minimumsupport, transactions);
	}

	public VIntConcatenatedDataset(IterableDataset parent, int extension,
			int[] ignoreItems) throws DontExploreThisBranchException {
		super(parent, extension, ignoreItems);
	}

	public VIntConcatenatedDataset(IterableDataset parent, int extension)
			throws DontExploreThisBranchException {
		super(parent, extension);
	}

	@Override
	protected void prepareTransactionsStructure(int remainingItemsCount) {
		int remainingItemsSize = 0;
		TIntIntIterator counts = this.supportCounts.iterator();
		while (counts.hasNext()) {
			counts.advance();
			remainingItemsSize += getVIntSize(counts.key()) * counts.value();
		}
		this.concatenated = new byte[remainingItemsSize + nbBytesForTransSize
				* this.transactionsCount];
	}

	@Override
	public Dataset getProjection(int extension)
			throws DontExploreThisBranchException {
		double extensionSupport = this.supportCounts.get(extension);
		if ((extensionSupport / this.transactionsCount) > UnfilteredDataset.FILTERING_THRESHOLD) {
			return new VIntConcatenatedUnfilteredDataset(this, extension);
		} else {
			return new VIntConcatenatedDataset(this, extension);
		}
	}

	@Override
	public ExtensionsIterator getCandidatesIterator() {
		return new CandidatesIterator();
	}

	static public int readVInt(byte[] array, IntHolder pointer) {
		byte b = array[pointer.value];
		pointer.value++;
		if (b >= 0) {
			return (int) b;
		} else {
			int res = (b & 0x7F);
			int shift = 7;
			while (true) {
				b = array[pointer.value];
				pointer.value++;
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

	static public void writeVInt(byte[] array, IntHolder pointer, int value) {
		while (true) {
			if (value >= 0 && value < 0x00000080) {
				array[pointer.value] = (byte) value;
				// System.out.println("encoding "
				// + String.format("%X", (byte) value));
				pointer.value++;
				break;
			} else {
				// System.out.println("encoding "
				// + String.format("%X", ((byte) value)) + " into "
				// + String.format("%X", (((byte) value) | 0x80)));
				array[pointer.value] = (byte) (((byte) value) | 0x80);
				value = value >>> 7;
				pointer.value++;
			}
		}
	}

	static public int getVIntSize(int val) {
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

	@Override
	protected TIntIterator readTransaction(int tid) {
		return new ItemsIterator(tid);
	}

	@Override
	protected TransactionsWriter getTransactionsWriter() {
		return new TransWriter();
	}

	private class ItemsIterator implements TIntIterator {
		private int tid;
		private IntHolder index;

		public ItemsIterator(int tid) {
			this.tid = tid;
			this.index = new IntHolder(tid);
			int length = readVInt(concatenated, this.index);
			this.index.value = tid - length;
		}

		@Override
		public boolean hasNext() {
			return index.value < tid;
		}

		@Override
		public void remove() {
			throw new NotImplementedException();
		}

		@Override
		public int next() {
			return readVInt(concatenated, this.index);
		}

	}

	private class TransWriter implements TransactionsWriter {
		IntHolder index = new IntHolder(0);
		int transactionStart = 0;

		public TransWriter() {
		}

		@Override
		public void addItem(int item) {
			writeVInt(concatenated, this.index, item);
		}

		@Override
		public int endTransaction() {
			int transId = this.index.value;
			writeVInt(concatenated, this.index, this.index.value
					- this.transactionStart);
			this.transactionStart = this.index.value;
			return transId;
		}

	}

	@Override
	public int getRealSize() {
		return this.concatenated.length;
	}

	// public static void main(String[] args) throws Exception {
	// // int[] tests = { 127, 128, 2097152 - 1, 2097152, Integer.MAX_VALUE,
	// // Integer.MIN_VALUE };
	// // for (int t : tests) {
	// // setMaxTransactionLength(t);
	// // System.out.println(nbBytesForTransSize + " " + getVIntSize(t));
	// // System.out.println(String.format("%X", t));
	// // }
	// // IntHolder ih = new IntHolder(0);
	// // byte[] tab = new byte[5];
	// // int val = 127;
	// // System.out.println(String.format("%X", val));
	// // System.out.println(val);
	// // writeVInt(tab, ih, val);
	// // for (byte b : tab) {
	// // System.out.print(String.format("%X", b));
	// // }
	// // System.out.println();
	// // System.out.println(ih.value);
	// // ih.value = 0;
	// // System.out.println(readVInt(tab, ih));
	// // System.out.println(ih.value);
	// FilteredDataset d = new RebasedVIntConcatenatedDataset(5,
	// new FileReader(
	// "/Users/vleroy/Workspace/lastfm/lastfm-s1200.dat"));
	// System.out.println(d.getRealSize());
	// d = null;
	// d = new VIntConcatenatedDataset(5, new FileReader(
	// "/Users/vleroy/Workspace/lastfm/lastfm-s1200.dat"));
	// System.out.println(d.getRealSize());
	// d = null;
	// VIntConcatenatedDataset.setMaxTransactionLength(200);
	// d = new RebasedVIntConcatenatedDataset(5, new FileReader(
	// "/Users/vleroy/Workspace/lastfm/lastfm-s1200.dat"));
	// System.out.println(d.getRealSize());
	// d = null;
	// d = new RebasedConcatenatedDataset(5, new FileReader(
	// "/Users/vleroy/Workspace/lastfm/lastfm-s1200.dat"));
	// System.out.println(d.getRealSize());
	// d = null;
	// }
}
