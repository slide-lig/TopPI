package fr.liglab.lcm.tests;

import static org.junit.Assert.*;

import org.junit.Test;

import fr.liglab.lcm.LCM;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.ExtensionsIterator;
import fr.liglab.lcm.internals.RebasedConcatenatedDataset;
import fr.liglab.lcm.io.FileReader;
import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.io.PerItemTopKCollector;
import fr.liglab.lcm.io.RebaserCollector;
import fr.liglab.lcm.tests.stubs.NullCollector;
import fr.liglab.lcm.tests.stubs.StubPatternsCollector;
import gnu.trove.map.TIntIntMap;

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
	public void testCollect() {
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
	
	@Test
	public void testCompleteLCM() {
		FileReader trans = FileReaderTest.getTestExplore();
		RebasedConcatenatedDataset dataset = new RebasedConcatenatedDataset(2, trans);
		
		PatternsCollector collector = FileReaderTest.getTestExplorePatternsK2();
		collector = new RebaserCollector(collector, dataset);
		collector = new PerItemTopKCollector(collector, 2, true);
		
		LCM instance = new LCM(collector);
		instance.lcm(dataset);
		collector.close();
	}
	
	@Test
	public void testExplore() {
		FileReader trans = FileReaderTest.getTestExplore();
		RebasedConcatenatedDataset dataset = new RebasedConcatenatedDataset(2, trans);
		
		// rebasing : 1 => 0 , 2 => 1, 3 => 2
		PatternsCollector collector = new NullCollector();
		collector = new PerItemTopKCollector(collector, 2, true);
		
		// exploring 0 and 1
		collector.collect(8, new int[] {0});
		collector.collect(5, new int[] {1});
		collector.collect(3, new int[] {0, 1});
		
		// outputted candidates should be 0 and 1
		Dataset dataset_2 = dataset.getProjection(2);
		ExtensionsIterator it_2 = dataset_2.getCandidatesIterator();
		int[] freqs_2 = it_2.getSortedFrequents();
		TIntIntMap supports_2 = dataset_2.getSupportCounts();
		
		collector.collect(4, new int[] {2});
		
		assertTrue(collector.explore(new int[] {2}, 0, freqs_2, supports_2));
		collector.collect(3, new int[] {2,0});
		
		// next one should be 2,{2,1} - which support is too low 
		assertFalse(collector.explore(new int[] {2}, 1, freqs_2, supports_2));
	}
}
