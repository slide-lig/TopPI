package fr.liglab.lcm.tests.matchers;

import java.util.Arrays;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;

/**
 * usage : 
 * 
 * import static fr.liglab.lcm.tests.matchers.ItemsetsAsArraysMatcher.arrayIsItemset
 * assertThat(myIntArray, arrayIsItemset(new int[] {1,2,3}))
 */
public class ItemsetsAsArraysMatcher extends BaseMatcher<int[]> {
	
	int[] expected;

	@Factory
	public static ItemsetsAsArraysMatcher arrayIsItemset(int[] expectation) {
		return new ItemsetsAsArraysMatcher(expectation);
	}
	
	private ItemsetsAsArraysMatcher(int[] expectation) {
		expected = expectation;
		Arrays.sort(expected);
	}

	public boolean matches(Object arg0) {
		int[] itemset = (int[]) arg0;
		
		if (itemset.length != expected.length) {
			return false;
		} else {
			int[] sorted = Arrays.copyOf(itemset, itemset.length);
			Arrays.sort(sorted);
			
			for (int i = 0; i < itemset.length; i++) {
				if (expected[i] != sorted[i]) {
					return false;
				}
			}
			
			return true;
		}
	}

	public void describeTo(Description description) {
		description.appendText("Expecting " + Arrays.toString(expected) + " (arrays have been sorted)");
	}
	
}
