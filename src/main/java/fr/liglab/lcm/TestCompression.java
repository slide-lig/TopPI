package fr.liglab.lcm;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import fr.liglab.lcm.internals.ConcatenatedCompressedDataset;
import fr.liglab.lcm.internals.ConcatenatedDataset;
import fr.liglab.lcm.internals.FPTreeMixDataset;
import fr.liglab.lcm.internals.RebasedFPTreeMixDataset;
import fr.liglab.lcm.io.FileReader;

public class TestCompression {

	/**
	 * @param args
	 * @throws DontExploreThisBranchException
	 */
	public static void main(String[] args) throws DontExploreThisBranchException {
		int minsup = Integer.parseInt(args[1]);
		FileReader reader = new FileReader(args[0]);

		ConcatenatedCompressedDataset compressed = new ConcatenatedCompressedDataset(minsup, reader);

		System.out.println("Realsize = " + compressed.getRealSize());
		System.out.println("computeTransactionsCount=" + compressed.computeTransactionsCount());
		System.out.println("getTransactionsCount=" + compressed.getTransactionsCount());
		System.out.println("getErasedItemsCount=" + compressed.getErasedItemsCount());
		compressed = null;

		reader = new FileReader(args[0]);
		ConcatenatedDataset normal = new ConcatenatedDataset(minsup, reader);
		System.out.println("Realsize = " + normal.getRealSize());
		normal = null;

		reader = new FileReader(args[0]);
		FPTreeMixDataset fpmix = new FPTreeMixDataset(minsup, reader);
		System.out.println("Realsize = " + fpmix.getRealSize());
		fpmix = null;

		reader = new FileReader(args[0]);
		RebasedFPTreeMixDataset rfpmix = new RebasedFPTreeMixDataset(minsup, reader);
		System.out.println("Realsize = " + rfpmix.getRealSize());
		rfpmix = null;
	}

}
