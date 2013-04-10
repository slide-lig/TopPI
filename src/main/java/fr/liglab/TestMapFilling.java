package fr.liglab;

import gnu.trove.map.hash.TIntIntHashMap;

public class TestMapFilling {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		long start = System.currentTimeMillis();
		
		int target = Integer.parseInt(args[0]);
		TIntIntHashMap map = new TIntIntHashMap();
		
		for (int i = 0; i < target; i++) {
			map.put(i, target-i);
		}
		
		
		System.out.println(target+" entries added in "+(System.currentTimeMillis() - start)+"ms");
	}

}
