package fr.liglab.lcm.tests;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import fr.liglab.lcm.io.FileReader;
import fr.liglab.lcm.tests.stubs.StubPatternsCollector;

/**
 * Its special feature is providing FileReaders and StubPatternsCollectors
 * on datasets in test/resources
 */
public class FileReaderTest {
	
	/**
	 * made for minsup=2
	 */
	public static FileReader getMicroReader() {
		return new FileReader("target/test-classes/micro.dat");
	}
	
	/**
	 * minsup=2
	 */
	public static StubPatternsCollector getMicroReaderPatterns() {
		StubPatternsCollector patterns = new StubPatternsCollector();
		patterns.expectCollect(3, 	1, 6, 5, 3);
		patterns.expectCollect(2, 	1, 6, 5, 3, 7);
		patterns.expectCollect(2, 	2, 3);
		patterns.expectCollect(4, 	3);
		patterns.expectCollect(3, 	3, 7);
		patterns.expectCollect(4, 	5);
		patterns.expectCollect(3, 	5, 7);
		patterns.expectCollect(4, 	7);
		return patterns;
	}
	
	/**
	 * made for minsup=2
	 */
	public static FileReader getGlobalClosure() {
		return new FileReader("target/test-classes/globalclosure.dat");
	}
	
	/**
	 * minsup=2
	 */
	public static StubPatternsCollector getGlobalClosurePatterns() {
		StubPatternsCollector patterns = new StubPatternsCollector();
		patterns.expectCollect(5, 	1);
		patterns.expectCollect(3, 	1, 2);
		return patterns;
	}
	
	/**
	 * made for minsup=2
	 */
	public static FileReader getFakeGlobalClosure() {
		return new FileReader("target/test-classes/fakeglobalclosure.dat");
	}

	/**
	 * minsup=2
	 */
	public static StubPatternsCollector getFakeGlobalClosurePatterns() {
		StubPatternsCollector patterns = new StubPatternsCollector();
		patterns.expectCollect(4, 	1);
		patterns.expectCollect(3, 	1, 2);
		return patterns;
	}
	
	
	
	
	
	@Test
	/**
	 * note : the empty line in micro.dat *is intentional*
	 */
	public void testMicroLoading() {
		FileReader reader = FileReaderTest.getMicroReader();
		assertTrue(reader.hasNext());
		assertArrayEquals(new int[] {5,3,1,6,7}, reader.next());
		assertTrue(reader.hasNext());
		assertArrayEquals(new int[] {5,3,1,2,6}, reader.next());
		assertTrue(reader.hasNext());
		assertArrayEquals(new int[] {5,7}, reader.next());
		assertTrue(reader.hasNext());
		assertArrayEquals(new int[] {3,2,7}, reader.next());
		assertTrue(reader.hasNext());
		assertArrayEquals(new int[] {5,3,1,6,7}, reader.next());
		assertFalse(reader.hasNext());
	}

}
