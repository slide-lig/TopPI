package fr.liglab.lcm.internals;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;

public class ConcatenatedUnfilteredCompressedDataset extends UnfilteredDataset {
	public ConcatenatedUnfilteredCompressedDataset(IterableDataset parentDataset, int extension)
			throws DontExploreThisBranchException {
		super(parentDataset, extension);
	}

	public ConcatenatedUnfilteredCompressedDataset(UnfilteredDataset upper, int extension)
			throws DontExploreThisBranchException {
		super(upper, extension);
	}

	@Override
	public Dataset createUnfilteredDataset(UnfilteredDataset upper, int extension)
			throws DontExploreThisBranchException {
		return new ConcatenatedUnfilteredCompressedDataset(upper, extension);
	}

	@Override
	public Dataset createFilteredDataset(IterableDataset upper, int extension, int[] ignoredItems)
			throws DontExploreThisBranchException {
		return new ConcatenatedCompressedDataset(upper, extension, ignoredItems);
	}

}
