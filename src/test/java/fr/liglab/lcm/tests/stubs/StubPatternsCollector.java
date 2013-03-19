package fr.liglab.lcm.tests.stubs;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import fr.liglab.lcm.io.PatternsCollector;

/**
 * Firstly, fill expected patterns with expectCollect()
 * Then invoke LCM with this collector
 * 
 * a RuntimeException is thrown :
 * - if an unexpected pattern appears
 * - if one (or more) expected pattern(s) hasn't been seen on close() invocation
 * 
 * You should also assertTrue(thisCollector.isEmpty()) at the end 
 */
public class StubPatternsCollector extends PatternsCollector {
	
	protected static Map<Integer, Set<Set<Integer>>> expected = new TreeMap<Integer, Set<Set<Integer>>>();
	protected long collected = 0;
	
	public void expectCollect(Integer support, Integer... patternItems) {
		Set<Set<Integer>> supportExpectation = null;
		
		if (expected.containsKey(support)){
			supportExpectation = expected.get(support);
		} else {
			supportExpectation = new HashSet<Set<Integer>>();
			expected.put(support, supportExpectation);
		}
		
		supportExpectation.add(new TreeSet<Integer>( Arrays.asList(patternItems)));
	}
	
	public boolean isEmpty() {
		return expected.isEmpty();
	}

	public void collect(int support, int[] pattern) {
		Set<Integer> p = new TreeSet<Integer>();
		for (int item : pattern) {
			p.add(item);
		}
		
		if (expected.containsKey(support)) {
			Set<Set<Integer>> expectations = expected.get(support);
			
			if (expectations.contains(p)) {
				expectations.remove(p);
				if (expectations.isEmpty()) {
					expected.remove(support);
				}
				this.collected++;
				return;
			}
		}
		
		throw new RuntimeException("Unexpected support/pattern : " + p.toString() + " , support=" + support);
	}

	public long close() {
		if (!isEmpty()) {
			StringBuilder builder = new StringBuilder();
			builder.append("Expected pattern(s) not found :\n");
			
			for (Integer support: expected.keySet()) {
				Set<Set<Integer>> supportExpectation = expected.get(support);
				for (Set<Integer> pattern : supportExpectation) {
					builder.append(pattern.toString());
					builder.append(", support = ");
					builder.append(support);
					builder.append("\n");
				}
			}
			
			throw new RuntimeException(builder.toString());
		}
		
		return this.collected;
	}
}
