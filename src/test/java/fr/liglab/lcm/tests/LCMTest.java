package fr.liglab.lcm.tests;

import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import fr.liglab.lcm.LCM;
import fr.liglab.lcm.internals.BasicDataset;
import fr.liglab.lcm.internals.ConcatenatedDataset;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.RebasedConcatenatedDataset;
import fr.liglab.lcm.internals.RebasedBasicDataset;
import fr.liglab.lcm.io.RebaserCollector;
import fr.liglab.lcm.tests.stubs.StubPatternsCollector;

public class LCMTest {
	
	private void invokeAndCheck(StubPatternsCollector collector, Dataset dataset) {
		LCM instance = new LCM(collector);
		instance.lcm(dataset);
		assertTrue(collector.isEmpty());
	}
	
	private void invokeRebasedBasicAndCheck(int minsup, StubPatternsCollector collector, Iterator<int[]> transactions) {
		RebasedBasicDataset dataset = new RebasedBasicDataset(minsup, transactions);
		RebaserCollector wrapper = new RebaserCollector(collector, dataset);
		
		LCM instance = new LCM(wrapper);
		instance.lcm(dataset);
		assertTrue(collector.isEmpty());
	}

	private void invokeRebasedConcatenatedAndCheck(int minsup, StubPatternsCollector collector, Iterator<int[]> transactions) {
		RebasedConcatenatedDataset dataset = new RebasedConcatenatedDataset(minsup, transactions);
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
		invokeRebasedBasicAndCheck(2,
				FileReaderTest.getFakeGlobalClosurePatterns(),
				FileReaderTest.getFakeGlobalClosure()
				);
		
		invokeRebasedBasicAndCheck(2,
				FileReaderTest.getGlobalClosurePatterns(),
				FileReaderTest.getGlobalClosure()
				);
		
		invokeRebasedBasicAndCheck(2,
				FileReaderTest.getMicroReaderPatterns(),
				FileReaderTest.getMicroReader()
				);
		
		invokeRebasedBasicAndCheck(4,
				FileReaderTest.get50RetailPatterns(),
				FileReaderTest.get50Retail()
				);
		
		invokeRebasedBasicAndCheck(2,
				FileReaderTest.getRebasingPatterns(),
				FileReaderTest.getRebasing()
				);
	}
	
	@Test
	public void testOnConcatenatedDataset() {
		invokeAndCheck(
				FileReaderTest.getFakeGlobalClosurePatterns(),
				new ConcatenatedDataset(2,FileReaderTest.getFakeGlobalClosure())
				);
		
		invokeAndCheck(
				FileReaderTest.getGlobalClosurePatterns(),
				new ConcatenatedDataset(2, FileReaderTest.getGlobalClosure())
				);
	
		invokeAndCheck(
				FileReaderTest.getMicroReaderPatterns(),
				new ConcatenatedDataset(2, FileReaderTest.getMicroReader())
				);
		
		invokeAndCheck(
				FileReaderTest.get50RetailPatterns(),
				new ConcatenatedDataset(4, FileReaderTest.get50Retail())
				);
	}
	
	@Test
	public void testOnRebasedConcatenatedDataset() {
		invokeRebasedConcatenatedAndCheck(2,
				FileReaderTest.getFakeGlobalClosurePatterns(),
				FileReaderTest.getFakeGlobalClosure()
				);
		
		invokeRebasedConcatenatedAndCheck(2,
				FileReaderTest.getGlobalClosurePatterns(),
				FileReaderTest.getGlobalClosure()
				);
		
		invokeRebasedConcatenatedAndCheck(2,
				FileReaderTest.getMicroReaderPatterns(),
				FileReaderTest.getMicroReader()
				);
		
		invokeRebasedConcatenatedAndCheck(4,
				FileReaderTest.get50RetailPatterns(),
				FileReaderTest.get50Retail()
				);
		
		invokeRebasedConcatenatedAndCheck(2,
				FileReaderTest.getRebasingPatterns(),
				FileReaderTest.getRebasing()
				);
	}
}
