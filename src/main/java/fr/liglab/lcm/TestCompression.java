package fr.liglab.lcm;

import fr.liglab.lcm.internals.ConcatenatedCompressedDataset;
import fr.liglab.lcm.internals.ConcatenatedDataset;
import fr.liglab.lcm.io.FileReader;

public class TestCompression {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int minsup = Integer.parseInt(args[1]);
		FileReader reader = new FileReader(args[0]);
		
		ConcatenatedCompressedDataset compressed = new ConcatenatedCompressedDataset(minsup, reader);
		
		System.out.println("Realsize = " + compressed.getRealSize());
		System.out.println("computeTransactionsCount=" + compressed.computeTransactionsCount());
		System.out.println("transactionsAtZero=" + compressed.transactionsAtZero);
		System.out.println("getTransactionsCount=" + compressed.getTransactionsCount());
		compressed = null;
		
		reader = new FileReader(args[0]);
		
		ConcatenatedDataset normal = new ConcatenatedDataset(minsup, reader);
		System.out.println("Realsize = " + normal.getRealSize());
	}

}
