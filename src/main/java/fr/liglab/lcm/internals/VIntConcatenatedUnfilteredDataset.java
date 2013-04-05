package fr.liglab.lcm.internals;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;

public class VIntConcatenatedUnfilteredDataset extends UnfilteredDataset {

	public VIntConcatenatedUnfilteredDataset(IterableDataset parentDataset,
			int extension) throws DontExploreThisBranchException {
		super(parentDataset, extension);
	}

	public VIntConcatenatedUnfilteredDataset(UnfilteredDataset upper,
			int extension) throws DontExploreThisBranchException {
		super(upper, extension);
	}

	@Override
	public Dataset createUnfilteredDataset(UnfilteredDataset upper,
			int extension) throws DontExploreThisBranchException {
		return new VIntConcatenatedUnfilteredDataset(upper, extension);
	}

	@Override
	public Dataset createFilteredDataset(IterableDataset upper, int extension,
			int[] ignoredItems) throws DontExploreThisBranchException {
		return new VIntConcatenatedDataset(upper, extension, ignoredItems);
	}

}
