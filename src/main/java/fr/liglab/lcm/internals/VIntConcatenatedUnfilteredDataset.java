package fr.liglab.lcm.internals;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import gnu.trove.list.array.TIntArrayList;

public class VIntConcatenatedUnfilteredDataset extends UnfilteredDataset {

	public VIntConcatenatedUnfilteredDataset(IterableDataset parentDataset,
			int extension) throws DontExploreThisBranchException {
		super(parentDataset, extension);
	}

	public VIntConcatenatedUnfilteredDataset(UnfilteredDataset upper,
			int extension, TIntArrayList extensionTids)
			throws DontExploreThisBranchException {
		super(upper, extension, extensionTids);
	}

	@Override
	public Dataset createUnfilteredDataset(UnfilteredDataset upper,
			int extension, TIntArrayList extensionTid)
			throws DontExploreThisBranchException {
		return new VIntConcatenatedUnfilteredDataset(upper, extension,
				extensionTid);
	}

	@Override
	public Dataset createFilteredDataset(IterableDataset upper, int extension,
			TIntArrayList extensionTids, int[] ignoredItems)
			throws DontExploreThisBranchException {
		return new VIntConcatenatedDataset(upper, extension, extensionTids,
				ignoredItems);
	}

}
