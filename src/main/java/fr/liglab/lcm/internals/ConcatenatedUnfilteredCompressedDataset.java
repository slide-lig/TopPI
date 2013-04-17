package fr.liglab.lcm.internals;

import com.google.common.primitives.Ints;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;

public class ConcatenatedUnfilteredCompressedDataset extends UnfilteredDataset {
	public ConcatenatedUnfilteredCompressedDataset(IterableDataset parentDataset, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		super(parentDataset, extension, ignoreItems);
	}

	public ConcatenatedUnfilteredCompressedDataset(UnfilteredDataset upper, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		super(upper, extension, ignoreItems);
	}

	@Override
	public Dataset createUnfilteredDataset(UnfilteredDataset upper, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		return new ConcatenatedUnfilteredCompressedDataset(upper, extension, ignoreItems);
	}

	@Override
	public Dataset createFilteredDataset(IterableDataset upper, int extension, int[] ignoredItems)
			throws DontExploreThisBranchException {
		return new ConcatenatedCompressedDataset(upper, extension, Ints.concat(this.ignoreItems, ignoredItems));
	}

}
