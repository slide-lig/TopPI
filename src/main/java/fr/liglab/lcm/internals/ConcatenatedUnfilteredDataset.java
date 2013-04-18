package fr.liglab.lcm.internals;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;

public class ConcatenatedUnfilteredDataset extends UnfilteredDataset {
	public ConcatenatedUnfilteredDataset(IterableDataset parentDataset,
			int extension) throws DontExploreThisBranchException {
		super(parentDataset, extension);
	}

	public ConcatenatedUnfilteredDataset(UnfilteredDataset upper, int extension)
			throws DontExploreThisBranchException {
		super(upper, extension);
	}

	@Override
	public Dataset createUnfilteredDataset(UnfilteredDataset upper,
			int extension) throws DontExploreThisBranchException {
		return new ConcatenatedUnfilteredDataset(upper, extension);
	}

	@Override
	public Dataset createFilteredDataset(IterableDataset upper, int extension,
			int[] ignoredItems) throws DontExploreThisBranchException {
		return new ConcatenatedDataset(upper, extension, ignoredItems);
	}

}
