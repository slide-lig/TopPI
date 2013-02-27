package fr.liglab.lcm.tests;

import static org.junit.Assert.*;

import org.junit.Test;

import fr.liglab.lcm.io.PerItemTopKCollector;
import fr.liglab.lcm.tests.stubs.StubPatternsCollector;

public class PerItemTopKCollectorTest {
	
	/**
	 * just to check this doesn't raise exceptions
	 */
	@Test
	public void testEmpty() {
		StubPatternsCollector stub = new StubPatternsCollector();
		PerItemTopKCollector collector = new PerItemTopKCollector(stub, 50, true);
		collector.close();
	}
	
	/**
	 * a complete test on a small file
	 */
	@Test
	public void test() {
		StubPatternsCollector stub = new StubPatternsCollector();
		
		PerItemTopKCollector collector = new PerItemTopKCollector(stub, 2, true);
		collector.collect(100, new int[] {0});
		collector.collect(90,  new int[] {1});
		collector.collect(20,  new int[] {1, 0});
		collector.collect(80,  new int[] {2});
		collector.collect(70,  new int[] {2, 1});
		collector.collect(80,  new int[] {3});
		collector.collect(20,  new int[] {3, 0});
		collector.collect(50,  new int[] {3, 2, 1});
		collector.collect(60,  new int[] {3, 2});
		collector.collect(10,  new int[] {4});
		
		// so far nothing should have happen to stub
		stub.expectCollect(100,	0);
		stub.expectCollect(90,	1);
		stub.expectCollect(80,	2);
		stub.expectCollect(80,	3);
		stub.expectCollect(70,	2, 1); // 2 and 1 full
		stub.expectCollect(60,	3, 2); // 3 full
		stub.expectCollect(20,	1, 0); // 0 full
		stub.expectCollect(10,	4);
		collector.close();
		
		assertTrue(stub.isEmpty());
	}

}
