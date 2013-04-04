package fr.liglab.lcm.internals;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import gnu.trove.list.array.TIntArrayList;

public class ConcatenatedUnfilteredDataset extends UnfilteredDataset {
	public ConcatenatedUnfilteredDataset(IterableDataset parentDataset,
			int extension) throws DontExploreThisBranchException {
		super(parentDataset, extension);
	}

	public ConcatenatedUnfilteredDataset(UnfilteredDataset upper,
			int extension, TIntArrayList extensionTids)
			throws DontExploreThisBranchException {
		super(upper, extension, extensionTids);
	}

	@Override
	public Dataset createUnfilteredDataset(UnfilteredDataset upper,
			int extension, TIntArrayList extensionTid)
			throws DontExploreThisBranchException {
		return new ConcatenatedUnfilteredDataset(upper, extension, extensionTid);
	}

	@Override
	public Dataset createFilteredDataset(IterableDataset upper, int extension,
			TIntArrayList extensionTids, int[] ignoredItems)
			throws DontExploreThisBranchException {
		return new ConcatenatedDataset(upper, extension, extensionTids,
				ignoredItems);
	}

}
