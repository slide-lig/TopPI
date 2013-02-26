package fr.liglab.lcm.tests;

import static org.junit.Assert.*;
import static fr.liglab.lcm.tests.matchers.ItemsetsAsArraysMatcher.arrayIsItemset;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.liglab.lcm.util.ItemsetsFactory;

public class ItemsetsFactoryTest {
	
	ItemsetsFactory factory;

	@Before
	public void setUp() throws Exception {
		factory = new ItemsetsFactory();
	}

	@After
	public void tearDown() throws Exception {
		factory = null;
	}
	
	@Test
	public void testEmpty() {
		assertTrue(factory.isEmpty());
		int[] result = factory.get();
		assertEquals(0, result.length);
		factory.add(1);
		assertFalse(factory.isEmpty());
	}

	@Test
	public void testBuildingAndOrdering() {
		for (int i = 0; i < 10; i++) {
			factory.add(i);
		}
		
		int[] result = factory.get();
		assertEquals(10, result.length);
		
		for (int i = 0; i < 10; i++) {
			assertEquals(i, result[i]);
		}
	}

	@Test
	public void testLooooongArray() {
		for (int i = 10000; i > 0; i--) {
			factory.add(i);
		}
		
		int[] result = factory.get();
		assertEquals(10000, result.length);
		assertEquals(10000, result[0]);
		assertEquals(1, result[result.length-1]);
	}
	
	@Test
	public void testMultipleInstanciations() {
		int[] result = factory.get();
		assertEquals(0, result.length);
		
		factory.add(1);
		result = factory.get();
		assertEquals(1, result.length);
		assertEquals(1, result[0]);
		
		testLooooongArray();
		testBuildingAndOrdering();
	}
	
	@Test
	public void testExtend() {
		int[] extended = ItemsetsFactory.extend(new int[0], 4, new int[0]);
		assertThat(extended, arrayIsItemset(new int[] {4}));
		
		extended = ItemsetsFactory.extend(new int[] {1,2,3}, 4, new int[0]);
		assertThat(extended, arrayIsItemset(new int[] {1,2,3,4}));
		
		extended = ItemsetsFactory.extend(new int[0], 4, new int[] {5,6,7});
		assertThat(extended, arrayIsItemset(new int[] {4,5,6,7}));
		
		extended = ItemsetsFactory.extend(new int[] {1,2,3}, 4, new int[] {5,6,7});
		assertThat(extended, arrayIsItemset(new int[] {1,2,3,4,5,6,7}));
	}
}
