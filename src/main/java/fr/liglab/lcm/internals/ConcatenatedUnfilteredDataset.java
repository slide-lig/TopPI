package fr.liglab.lcm.internals;

import com.google.common.primitives.Ints;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;

public class ConcatenatedUnfilteredDataset extends UnfilteredDataset {
	public ConcatenatedUnfilteredDataset(IterableDataset parentDataset, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		super(parentDataset, extension, ignoreItems);
	}

	public ConcatenatedUnfilteredDataset(UnfilteredDataset upper, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		super(upper, extension, ignoreItems);
	}

	@Override
	public Dataset createUnfilteredDataset(UnfilteredDataset upper, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		return new ConcatenatedUnfilteredDataset(upper, extension, ignoreItems);
	}

	@Override
	public Dataset createFilteredDataset(IterableDataset upper, int extension, int[] ignoredItems)
			throws DontExploreThisBranchException {
		return new ConcatenatedDataset(upper, extension, Ints.concat(this.ignoreItems, ignoredItems));
	}

}
