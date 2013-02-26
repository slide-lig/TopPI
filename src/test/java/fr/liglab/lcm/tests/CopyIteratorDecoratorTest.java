package fr.liglab.lcm.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;

import fr.liglab.lcm.util.CopyIteratorDecorator;

public class CopyIteratorDecoratorTest {
	
	@Test
	public void testReIteration() {
		ArrayList<Integer> source = new ArrayList<Integer>();
		source.add(12);
		source.add(23);
		source.add(34);
		
		CopyIteratorDecorator<Integer> tested = new CopyIteratorDecorator<Integer>(source.iterator());
		assertTrue(tested.hasNext());
		assertEquals((Integer) 12, tested.next());
		assertTrue(tested.hasNext());
		assertEquals((Integer) 23, tested.next());
		assertTrue(tested.hasNext());
		assertEquals((Integer) 34, tested.next());
		assertFalse(tested.hasNext());
		
		assertEquals(3, tested.size());
		
		Iterator<Integer> itAgain = tested.iterator();
		assertTrue(itAgain.hasNext());
		assertEquals((Integer) 12, itAgain.next());
		assertTrue(itAgain.hasNext());
		assertEquals((Integer) 23, itAgain.next());
		assertTrue(itAgain.hasNext());
		assertEquals((Integer) 34, itAgain.next());
		assertFalse(itAgain.hasNext());
		
		
	}
	
	@Test
	public void testRemove() {
		ArrayList<Integer> source = new ArrayList<Integer>();
		source.add(12);
		source.add(23);
		source.add(34);
		
		CopyIteratorDecorator<Integer> tested = new CopyIteratorDecorator<Integer>(source.iterator());
		assertTrue(tested.hasNext());
		assertEquals((Integer) 12, tested.next());
		assertTrue(tested.hasNext());
		assertEquals((Integer) 23, tested.next());
		tested.remove();
		assertTrue(tested.hasNext());
		assertEquals((Integer) 34, tested.next());
		assertFalse(tested.hasNext());
		
		assertEquals(2, tested.size());
		
		Iterator<Integer> itAgain = tested.iterator();
		assertTrue(itAgain.hasNext());
		assertEquals((Integer) 12, itAgain.next());
		assertTrue(itAgain.hasNext());
		assertEquals((Integer) 34, itAgain.next());
		assertFalse(itAgain.hasNext());
	}

}
