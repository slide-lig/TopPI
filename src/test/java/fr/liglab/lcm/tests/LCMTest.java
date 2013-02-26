package fr.liglab.lcm.tests;

import static org.junit.Assert.*;
import org.junit.Test;
import java.util.Iterator;

import fr.liglab.lcm.LCM;
import fr.liglab.lcm.internals.BasicDataset;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.RebasedBasicDataset;
import fr.liglab.lcm.io.RebaserCollector;
import fr.liglab.lcm.tests.stubs.StubPatternsCollector;

public class LCMTest {
	
	private void invokeAndCheck(StubPatternsCollector collector, Dataset dataset) {
		LCM instance = new LCM(collector);
		instance.lcm(dataset);
		assertTrue(collector.isEmpty());
	}
	
	private void invokeRebasedAndCheck(int minsup, StubPatternsCollector collector, Iterator<int[]> transactions) {
		RebasedBasicDataset dataset = new RebasedBasicDataset(minsup, transactions);
		RebaserCollector wrapper = new RebaserCollector(collector, dataset);
		
		LCM instance = new LCM(wrapper);
		instance.lcm(dataset);
		assertTrue(collector.isEmpty());
	}
	
	@Test
	public void testOnBasicDataset() {
		invokeAndCheck(
				FileReaderTest.getFakeGlobalClosurePatterns(),
				new BasicDataset(2,FileReaderTest.getFakeGlobalClosure())
				);
		
		invokeAndCheck(
				FileReaderTest.getGlobalClosurePatterns(),
				new BasicDataset(2, FileReaderTest.getGlobalClosure())
				);
	
		invokeAndCheck(
				FileReaderTest.getMicroReaderPatterns(),
				new BasicDataset(2, FileReaderTest.getMicroReader())
				);
		
		invokeAndCheck(
				FileReaderTest.get50RetailPatterns(),
				new BasicDataset(4, FileReaderTest.get50Retail())
				);
	}
	
	@Test
	public void testOnRebasedBasicDataset() {
		invokeRebasedAndCheck(2,
				FileReaderTest.getFakeGlobalClosurePatterns(),
				FileReaderTest.getFakeGlobalClosure()
				);
		
		invokeRebasedAndCheck(2,
				FileReaderTest.getGlobalClosurePatterns(),
				FileReaderTest.getGlobalClosure()
				);
		
		invokeRebasedAndCheck(2,
				FileReaderTest.getMicroReaderPatterns(),
				FileReaderTest.getMicroReader()
				);
		
		invokeRebasedAndCheck(4,
				FileReaderTest.get50RetailPatterns(),
				FileReaderTest.get50Retail()
				);
		
		invokeRebasedAndCheck(2,
				FileReaderTest.getRebasingPatterns(),
				FileReaderTest.getRebasing()
				);
	}
}
