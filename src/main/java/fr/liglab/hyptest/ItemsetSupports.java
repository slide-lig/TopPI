package fr.liglab.hyptest;

import java.util.Arrays;

public class ItemsetSupports {
	private int[] itemset;
	private int support;

	public ItemsetSupports(int[] itemset, int s1) {
		super();
		this.itemset = itemset;
		this.support = s1;
	}

	public final int[] getItemset() {
		return itemset;
	}

	/**
	 * @return the support count set at pattern insertion
	 */
	public final int getSupport() {
		return support;
	}
	
	@Override
	public String toString() {
		return "ItemsetSupport [itemset=" + Arrays.toString(itemset) + ", support=" + support + "]";
	}
}
