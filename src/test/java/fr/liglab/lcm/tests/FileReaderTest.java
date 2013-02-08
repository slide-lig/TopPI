package fr.liglab.lcm.tests;

import static org.junit.Assert.*;

import org.junit.Test;

import fr.liglab.lcm.io.FileReader;

public class FileReaderTest {

	@Test
	public void testMicroLoading() {
		FileReader reader = new FileReader("target/test-classes/micro.dat");
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
