package fr.liglab.mining.tests;

import static org.junit.Assert.*;

import org.junit.Test;

import fr.liglab.mining.internals.Counters;

public class CountersTest {

	@Test
	public void testSort1() {
		int[] supportCounts = new int[] {9,8,7,6,5,3,2,1};
		int[] reverseFromCompressed = Counters.sort(supportCounts, Integer.MAX_VALUE, supportCounts.length);
		assertArrayEquals(new int[] {9,8,7,6,5,3,2,1}, supportCounts);
		assertArrayEquals(new int[] {0,1,2,3,4,5,6,7}, reverseFromCompressed);
	}

	@Test
	public void testSort2() {
		int[] supportCounts = new int[] {-1,8,7,6,5,3,2,1};
		int[] reverseFromCompressed = Counters.sort(supportCounts, Integer.MAX_VALUE, supportCounts.length);
		assertArrayEquals(new int[] {8,7,6,5,3,2,1,-1}, supportCounts);
		assertArrayEquals(new int[] {1,2,3,4,5,6,7,-1}, reverseFromCompressed);
	}

	@Test
	public void testSort3() {
		int[] supportCounts = new int[] {-1,8,2,-1,8,3,2,1};
		int[] reverseFromCompressed = Counters.sort(supportCounts, Integer.MAX_VALUE, supportCounts.length);
		assertArrayEquals(new int[] {8,8,3,2,2,1,-1,-1}, supportCounts);
		assertArrayEquals(new int[] {1,4,5,2,6,7,-1,-4}, reverseFromCompressed);
	}

}
