package fr.liglab.lcm.tests;

import static org.junit.Assert.*;
import org.junit.Test;

import fr.liglab.lcm.LCM;
import fr.liglab.lcm.internals.BasicDataset;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.tests.stubs.StubPatternsCollector;

public class LCMTest {
	
	private void invokeAndCheck(StubPatternsCollector collector, Dataset dataset) {
		LCM instance = new LCM(collector);
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
		
	}
}
