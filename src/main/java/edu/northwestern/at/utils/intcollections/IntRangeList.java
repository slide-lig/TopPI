package edu.northwestern.at.utils.intcollections;

/*	Please see the license information at the end of this file. */

import java.util.*;
import java.io.*;

/**	A set of non-negative integers.
 *
 *	<p>The set is maintained as a list of integer ranges in increasing order
 *	with no overlap and with automatic coalescing of adjacent ranges. 
 *	
 *	<p>Inspired by the traditional Usenet "newsrc" format.
 */

public class IntRangeList implements Externalizable, Cloneable {

	/**	Serial version UID. */

	static final long serialVersionUID = 2458507879937096460L;

	/**	The list. */
	
	private LinkedList list = new LinkedList();
	
	/**	Constructs a new empty list.
	 */
	 
	public IntRangeList () {
	}
	
	/**	Constructs a new list with an initial single element.
	 *
	 *	@param	x		The initial single element.
	 */
	 
	public IntRangeList (int x) {
		list.add(new IntRange(x, x+1));
	}
	
	/**	Constructs a new list with an initial range.
	 *
	 *	@param	first	The first integer in the range.
	 *
	 *	@param	last	The last integer in the range + 1.
	 */
	 
	public IntRangeList (int first, int last) {
		if (first >= last) return;
		list.add(new IntRange(first, last));
	}
	
	/**	Constructs a new list from an IntRange object.
	 *
	 *	@param	range		The IntRange object.
	 */
	 
	public IntRangeList (IntRange range) {
		if (range.first >= range.last) return;
		list.add(new IntRange(range.first, range.last));
	}
		
	/**	Constructs a new list from a string representation.
	 *
	 *	<p>See {@link #toString} for a description of string representations
	 *	of integer range lists.
	 *
	 *	@param	str		The string.
	 *
	 *	@throws	IllegalArgumentException	If syntax error in string.
	 */
	
	public IntRangeList (String str) {
		if (str == null) return;
		StringTokenizer tokenizer = new StringTokenizer(str, ",");
		while (tokenizer.hasMoreTokens()) {
			String tok = tokenizer.nextToken();
			int first;
			int last;
			try {
				int i = tok.indexOf('-');
				if (i < 0) {
					first = Integer.parseInt(tok);
					last = first+1;
				} else {
					first = Integer.parseInt(tok.substring(0,i));
					last = Integer.parseInt(tok.substring(i+1)) + 1;
					if (first >= last) throw new IllegalArgumentException(
						"first >= last");
				}
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("syntax error");
			}
			list.add(new IntRange(first, last)); 
		}
	}
	
	/**	Returns an {@link IntIterator} for the list.
	 *
	 *	<p>The iterator returns the integer list elements in increasing order.
	 *	The "remove" method is not supported.
	 *
	 *	@return		An {@link IntIterator} for the list.
	 */
	 
	public IntIterator intIterator () {
		return new IntIterator () {
			private Iterator it = list.iterator();
			private IntRange range;
			private int next;
			public boolean hasNext () {
				if (range == null || next >= range.last) {
					return it.hasNext();
				} else {
					return true;
				}
			}
			public int next () {
				if (range == null || next >= range.last) {
					range = (IntRange)it.next();
					next = range.first;
				}
				return next++;
			}
		};
	}
	
	/**	Returns a range iterator for the list.
	 *
	 *	<p>The IntRange objects returned by the iterator are cloned. 
	 *	If they are modifed the list is unaffected. They are returned
	 *	in increasing order. The "remove" method is not supported.
	 *
	 *	@return		A range iterator for the list.
	 */
	 
	public Iterator rangeIterator () {
		return new Iterator() {
			private Iterator it = list.iterator();
			public boolean hasNext() {
				return it.hasNext ();
			}
			public Object next () {
				return ((IntRange)it.next()).clone();
			}
			public void remove () {
				throw new UnsupportedOperationException();
			}
		};
	}	
	
	/**	Returns the size of the list.
	 *
	 *	<p>The value returned is the number of integers in the list,
	 *	not the number of ranges.
	 *
	 *	@return		The size of the list.
	 */
	 
	public int size () {
		int result = 0;
		for (Iterator it = list.iterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			result += range.last - range.first;
		}
		return result;
	}
	
	/**	Returns true if the list is empty.
	 *
	 *	@return		True if list is empty.
	 */
	 
	public boolean isEmpty () {
		return list.isEmpty();
	}
	
	/**	Gets the i'th integer in the list.
	 *
	 *	@param	i		The index in the list of the integer to return.
	 *
	 *	@return			The i'th integer in the list (starting at 0), or
	 *					-1 if i is negative or greater than or equal to
	 *					the size of the list.
	 */
	 
	public int get (int i) {
		if (i < 0) return -1;
		for (Iterator it = list.iterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			int len = range.last - range.first;
			if (i < len) return range.first + i;
			i -= len;
		}
		return -1;
	}
	
	/**	Gets the index of a list element.
	 *
	 *	@param	x		The list element.
	 *
	 *	@return			The index of the element in the list, or -1
	 *					if the list does not contain the element.
	 */
	 
	public int indexOf (int x) {
		int ct = 0;
		for (Iterator it = list.iterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			if (range.first <= x && x < range.last)
				return ct + x - range.first;
			ct += range.last - range.first;
		}
		return -1;
	}
	
	/**	Gets the first integer in the list.
	 *
	 *	@return		The first element, or -1 if the list is empty.
	 */
	 
	public int first () {
		try {
			IntRange range = (IntRange)list.getFirst();
			return range.first;
		} catch (NoSuchElementException e) {
			return -1;
		}
	}
	
	/**	Gets the last integer in the list.
	 *
	 *	@return		The last element, or -1 if the list is empty.
	 */
	 
	public int last () {
		try {
			IntRange range = (IntRange)list.getLast();
			return range.last-1;
		} catch (NoSuchElementException e) {
			return -1;
		}
	}
	
	/**	Clears the list.
	 */
	
	public void clear () {
		list.clear();
	}
	
	/**	Sets the list to a range.
	 *
	 *	@param	first	The first integer in the range.
	 *
	 *	@param	last	The last integer in the range + 1.
	 */
	
	public void set (int first, int last) {
		list = new LinkedList();
		list.add(new IntRange(first, last));
	}
	
	/**	Adds an integer to the list.
	 *
	 *	@param	x	The integer
	 */
	
	public void add (int x) {
		int prevLast = -1;
		IntRange prev = null;
		for (ListIterator it = list.listIterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			if (x < range.last) {
				if (x >= range.first) return;
				if (x == prevLast) {
					if (x+1 == range.first) {
						prev.last = range.last;
						it.remove();
					} else {
						prev.last = x+1;
					}
				} else if (x+1 == range.first) {
					range.first = x;
				} else {
					it.previous();
					it.add(new IntRange(x, x+1));
				}
				return;
			}
			prev = range;
			prevLast = range.last;
		}
		if (x == prevLast) {
			prev.last++;
		} else {
			list.add(new IntRange(x, x+1));
		}
	}
	
	/**	Adds a range of integers to the list.
	 *
	 *	@param	first	The first integer in the range.
	 *
	 *	@param	last	The last integer in the range + 1.
	 */
	
	public void add (int first, int last) {
		if (first >= last) return;
		for (ListIterator it = list.listIterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			if (first <= range.last) {
				if (last < range.first) {
					it.previous();
					it.add(new IntRange(first, last));
					return;
				}
				if (first < range.first) range.first = first;
				IntRange range2 = range;
				while (true) {
					if (last <= range2.last) {
						range.last = range2.last;
						return;
					}
					if (!it.hasNext()) {
						range.last = last;
						return;
					}
					range2 = (IntRange)it.next();
					if (last < range2.first) {
						range.last = last;
						return;
					}
					it.remove();	
				}
			}
		}
		list.add(new IntRange(first, last));
	}
	
	/**	Adds an integer range to the list.
	 *
	 *	@param	range		The integer range.
	 */
	
	public void add (IntRange range) {
		add(range.first, range.last);
	}
	
	/**	Removes an integer from the list.
	 *
	 *	@param	x	The integer
	 */
	
	public void remove (int x) {
		for (ListIterator it = list.listIterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			if (x < range.first) return;
			if (x < range.last) {
				if (x == range.first) {
					if (x+1 == range.last) {
						it.remove();
					} else {
						range.first++;
					}
				} else if (x+1 == range.last) {
					range.last--;
				} else {
					it.add(new IntRange(x+1, range.last));
					range.last = x;
				}
				return;
			}
		}
	}
	
	/**	Removes a range of integers from the list.
	 *
	 *	@param	first	The first integer in the range.
	 *
	 *	@param	last	The last integer in the range + 1.
	 */
	
	public void remove (int first, int last) {
		if (first >= last) return;
		ListIterator it = list.listIterator();
		while (it.hasNext()) {
			IntRange range = (IntRange)it.next();
			if (first < range.last) {
				if (last <= range.first) return;
				if (last < range.last) {
					if (first <= range.first) {
						range.first = last;
					} else {
						it.add(new IntRange(last, range.last));
						range.last = first;
					}
					return;
				}
				if (first <= range.first) {
					it.remove();
				} else {
					range.last = first;
				}
			}
		}
	}
	
	/**	Removes an integer range from the list.
	 *
	 *	@param	range		The integer range.
	 */
	
	public void remove (IntRange range) {
		remove(range.first, range.last);
	}
	
	/**	Returns true if the list contains an integer.
	 *
	 *	@param	x	The integer.
	 *
	 *	@return		True if the list contains x.
	 */
	
	public boolean contains (int x) {
		for (Iterator it = list.iterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			if (range.first <= x && x < range.last) return true;
		}
		return false;
	}
	
	/**	Returns true if the list contains a range of integers.
	 *
	 *	@param	first	The first integer in the range.
	 *
	 *	@param	last	The last integer in the range + 1.
	 *
	 *	@return		True if the list contains the range.
	 */
	
	public boolean contains (int first, int last) {
		for (Iterator it = list.iterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			if (range.first <= first && last <= range.last) return true;
			if (first <= range.last) return false;
		}
		return false;
	}
	
	/**	Returns true if the list contains another list.
	 *
	 *	@param	other		The other list.
	 *
	 *	@return				True if this list contains the other list.
	 */
	 
	public boolean contains (IntRangeList other) {
		Iterator it = list.iterator();
		IntRange range = it.hasNext() ? (IntRange)it.next() : null;
		for (Iterator otherIt = other.list.iterator(); otherIt.hasNext(); ) {
			IntRange otherRange = (IntRange)otherIt.next();
			while (range != null && range.last < otherRange.last) {
				if (range.first > otherRange.first) return false;
				range = it.hasNext() ? (IntRange)it.next() : null;
			}
			if (range == null || otherRange.first < range.first) return false;
		}
		return true;
	}
	
	/**	Gets a right interval from the list.
	 *
	 *	<p>The interval returned is another IntRangeList which is a sublist
	 *	of this list. The sublist contains the specified left endpoint of
	 *	the interval x plus the delta-1 elements following the left endpoint.
	 *	If there aren't that many elements following the endpoint, as many
	 *	as possible are included.
	 *
	 *	@param	x			The left endpoint of the interval.
	 *
	 *	@param	delta		The number of elements after the endpoint + 1.
	 *
	 *	@return				The interval, or null if the list does not
	 *						contain x.
	 */
	 
	 public IntRangeList rightInterval (int x, int delta) {
		for (Iterator it = list.iterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			if (range.first <= x && x < range.last) {
				IntRangeList result = new IntRangeList();
				while (delta > 0) {
					int last = range.last;
					if (last - x > delta) last = x + delta;
					result.list.add(new IntRange(x, last));
					if (!it.hasNext()) break;
					delta -= last - x;
					range = (IntRange)it.next();
					x = range.first;
				}
				return result;
			}
		}
		return null;
	 }
	
	/**	Gets a left interval from the list.
	 *
	 *	<p>The interval returned is another IntRangeList which is a sublist
	 *	of this list. The sublist contains the delta elements preceding the
	 *	specified right endpoint x, but does not include x. If there aren't 
	 *	that many elements preceding the endpoint, as many as possible are 
	 *	included.
	 *
	 *	@param	x			The right endpoint of the interval.
	 *
	 *	@param	delta		The number of elements before the endpoint.
	 *
	 *	@return				The interval.
	 */
	 
	 public IntRangeList leftInterval (int x, int delta) {
		for (ListIterator it = list.listIterator(list.size()); 
			it.hasPrevious(); ) 
		{
			IntRange range = (IntRange)it.previous();
			if (range.first <= x) {
				IntRangeList result = new IntRangeList();
				while (delta > 0) {
					int first = range.first;
					if (x - first > delta) first = x - delta;
					if (first < x) result.list.addFirst(new IntRange(first, x));
					if (!it.hasPrevious()) break;
					delta -= x - first;
					range = (IntRange)it.previous();
					x = range.last;
				}
				return result;
			}
		}
		return null;
	 }
	
	/**	Gets a centered interval from the list.
	 *
	 *	<p>The interval returned is another IntRangeList which is a sublist
	 *	of this list. The sublist contains the delta elements preceding the
	 *	midpoint, the midpoint, and the delta-1 elements following the
	 *	midpoint. If there aren't that many elements preceding and/or
	 *	following the midpoint, as many as possible are included.
	 *
	 *	@param	x			The midpoint of the interval.
	 *
	 *	@param	delta		The range before and after the midpoint.
	 *
	 *	@return				The interval, or null if the list does not
	 *						contain x.
	 */
	 
	 public IntRangeList centerInterval (int x, int delta) {
	 	IntRangeList result = null;
	 	int centerPos = 0;
	 	int xSave = x;
	 	int deltaSave = delta;
		for (Iterator it = list.iterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			if (range.first <= x && x < range.last) {
				result = new IntRangeList();
				while (delta > 0) {
					int last = range.last;
					if (last - x > delta) last = x + delta;
					result.list.add(new IntRange(x, last));
					if (!it.hasNext()) break;
					delta -= last-x;
					range = (IntRange)it.next();
					x = range.first;
				}
				break;
			}
			centerPos++;
		}
		if (result == null) return null;
		x = xSave;
		delta = deltaSave;
		ListIterator it = list.listIterator(centerPos+1); 
		IntRange range = (IntRange)it.previous();
		boolean coalesce = true;
		while (delta > 0) {
			int first = range.first;
			if (x - first > delta) first = x - delta;
			if (first < x) {
				if (coalesce) {
					IntRange coalesceRange = (IntRange)result.list.getFirst();
					coalesceRange.first = first;
				} else {
					result.list.addFirst(new IntRange(first, x));
				}
			}
			coalesce = false;
			if (!it.hasPrevious()) break;
			delta -= x - first;
			range = (IntRange)it.previous();
			x = range.last;
		}
		return result;
	 }
	
	/**	Returns a new list which is the complement of the list.
	 *
	 *	@param	max		Maximum value + 1 for complemented list.
	 *
	 *	@return			The complement of the list.
	 */
	 
	public IntRangeList complement (int max) {
		IntRangeList result = new IntRangeList();
		int k = 0;
		for (Iterator it = list.iterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			if (k < range.first) 
				result.list.add(new IntRange(k, range.first));
			k = range.last;
		}
		result.add(k, max);
		return result;
	}
	
	/**	Returns a new list which is the union of this list and another list.
	 *
	 *	@param	other		The other list.
	 *
	 *	@return				The union of this list and the other list.
	 */
	 
	public IntRangeList union (IntRangeList other) {
		Iterator it1 = list.iterator();
		IntRange range1 = it1.hasNext() ? (IntRange)it1.next() : null;
		Iterator it2 = other.list.iterator();
		IntRange range2 = it2.hasNext() ? (IntRange)it2.next() : null;
		IntRangeList result = new IntRangeList();
		LinkedList resultList = result.list;
		int first = 0;
		int last = 0;
		while (range1 != null || range2 != null) {
			if (range2 == null || 
				range1 != null && range1.first < range2.first) 
			{
				first = range1.first;
				last = range1.last;
				range1 = it1.hasNext() ? (IntRange)it1.next() : null;
			} else {
				first = range2.first;
				last = range2.last;
				range2 = it2.hasNext() ? (IntRange)it2.next() : null;
			}
			while (true ) {
				if (range1 != null && range1.first <= last) {
					if (range1.last > last) last = range1.last;
					range1 = it1.hasNext() ? (IntRange)it1.next() : null;
				} else if (range2 != null && range2.first <= last) {
					if (range2.last > last) last = range2.last;
					range2 = it2.hasNext() ? (IntRange)it2.next() : null;
				} else {
					resultList.add(new IntRange(first, last));
					break;
				}
			}
		}
		return result;
	}
	
	/**	Returns a new list which is the intersection of this list with 
	 *	another list.
	 *
	 *	@param	other		The other list.
	 *
	 *	@return				This list intersected with the other list.
	 */
	 
	public IntRangeList intersect (IntRangeList other) {
		Iterator it1 = list.iterator();
		IntRange range1 = it1.hasNext() ? (IntRange)it1.next() : null;
		Iterator it2 = other.list.iterator();
		IntRange range2 = it2.hasNext() ? (IntRange)it2.next() : null;
		IntRangeList result = new IntRangeList();
		LinkedList resultList = result.list;
		while (range1 != null && range2 != null) {
			while (true) {
				while (range1 != null && range1.last <= range2.first)
					range1 = it1.hasNext() ? (IntRange)it1.next() : null;
				if (range1 == null) return result;
				while (range2 != null && range2.last <= range1.first)
					range2 = it2.hasNext() ? (IntRange)it2.next() : null;
				if (range2 == null) return result;
				if (range1.last > range2.first) break;
			}
			if (range2.first < range1.first) {
				if (range2.last < range1.last) {
					resultList.add(new IntRange(range1.first, range2.last));
					range2 = it2.hasNext() ? (IntRange)it2.next() : null;
				} else {
					resultList.add(new IntRange(range1.first, range1.last));
					range1 = it1.hasNext() ? (IntRange)it1.next() : null;
				}
			} else {
				if (range2.last < range1.last) {
					resultList.add(new IntRange(range2.first, range2.last));
					range2 = it2.hasNext() ? (IntRange)it2.next() : null;
				} else {
					resultList.add(new IntRange(range2.first, range1.last));
					range1 = it1.hasNext() ? (IntRange)it1.next() : null;
				}
			}
		}
		return result;
	}
	
	/**	Returns a new list which is this list minus another list.
	 *
	 *	@param	other		The other list.
	 *
	 *	@return				This list minus the other list.
	 */
	 
	public IntRangeList difference (IntRangeList other) {
		Iterator it1 = list.iterator();
		IntRange range1 = it1.hasNext() ? (IntRange)it1.next() : null;
		Iterator it2 = other.list.iterator();
		IntRange range2 = it2.hasNext() ? (IntRange)it2.next() : null;
		int first2 = 0;
		int last2 = range2 == null ? Integer.MAX_VALUE : range2.first;
		IntRangeList result = new IntRangeList();
		LinkedList resultList = result.list;
		while (range1 != null) {
			while (true) {
				while (range1 != null && range1.last <= first2)
					range1 = it1.hasNext() ? (IntRange)it1.next() : null;
				if (range1 == null) return result;
				while (last2 <= range1.first) {
					first2 = range2.last;
					range2 = it2.hasNext() ? (IntRange)it2.next() : null;
					last2 = range2 == null ? Integer.MAX_VALUE : range2.first;
				}
				if (range1.last > first2) break;
			}
			if (first2 < range1.first) {
				if (last2 < range1.last) {
					resultList.add(new IntRange(range1.first, last2));
					first2 = range2.last;
					range2 = it2.hasNext() ? (IntRange)it2.next() : null;
					last2 = range2 == null ? Integer.MAX_VALUE : range2.first;
				} else {
					resultList.add(new IntRange(range1.first, range1.last));
					range1 = it1.hasNext() ? (IntRange)it1.next() : null;
				}
			} else {
				if (last2 < range1.last) {
					resultList.add(new IntRange(first2, last2));
					first2 = range2.last;
					range2 = it2.hasNext() ? (IntRange)it2.next() : null;
					last2 = range2 == null ? Integer.MAX_VALUE : range2.first;
				} else {
					resultList.add(new IntRange(first2, range1.last));
					range1 = it1.hasNext() ? (IntRange)it1.next() : null;
				}
			}
		}
		return result;
	}
	
	/**	Returns a string representation of the list.
	 *
	 *	<p>The string representation is "xxx,xxx,xxx,...,xxx" where
	 *	each "xxx" is one integer "a" or is a range of integers "a-b".
	 *
	 *	<p>Note that the end of each range is inclusive in the string
	 *	representation, while it is exclusive in the API. For example,
	 *	a call to "add(10,20)" adds the integers 10 to but not including
	 *	20 to the list. When represented as a string the notation is
	 *	"10-19" meaning "10 through 19 inclusive."
	 *
	 *	@return		The string representation of the list.
	 */
	
	public String toString () {
		StringBuffer buf = new StringBuffer();
		Iterator it = list.iterator();
		while (it.hasNext()) {
			IntRange range = (IntRange)it.next();
			buf.append(range.first);
			if (range.last > range.first+1) {
				buf.append('-');
				buf.append(range.last-1);
			}
			buf.append(',');
		}
		int len = buf.length();
		return len == 0 ? "" : buf.substring(0,len-1);
	}
	
	/**	Returns a copy of the list.
	 *
	 *	@return		The copy.
	 */
	
	public Object clone () {
		IntRangeList result = new IntRangeList();
		for (Iterator it = list.iterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			result.list.add(new IntRange(range.first, range.last));
		}
		return result;
	}
	
	/**	Compares this list to another list.
	 *
	 *	@param	other		The other list.
	 *
	 *	@return				True if the lists are equal.
	 */
	 
	public boolean equals (Object other) {
		if (!(other instanceof IntRangeList)) return false;
		IntRangeList otherList = (IntRangeList)other;
		int n = list.size();
		if (n != otherList.list.size()) return false;
		Iterator it = list.iterator();
		Iterator otherIt = otherList.list.iterator();
		for (int i = 0; i < n; i++) {
			IntRange range = (IntRange)it.next();
			IntRange otherRange = (IntRange)otherIt.next();
			if (range.first != otherRange.first ||
				range.last != otherRange.last) return false;
		}
		return true;
	}
	
	/**	Writes the list on serialization.
	 *
	 *	@param	out		Object output stream.
	 *
	 *	@throws	IOException
	 */
	 
	public void writeExternal (ObjectOutput out)
		throws IOException
	{
		int numElements = list.size();
		out.writeInt(numElements);
		for (Iterator it = list.iterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			out.writeInt(range.first);
			out.writeInt(range.last);
		}
	}
	
	/**	Reads the list on deserialization.
	 *
	 *	@param	in		Object input stream.
	 *
	 *	@throws	IOException
	 *
	 *	@throws	ClassNotFoundException
	 */
	 
	public void readExternal (ObjectInput in)
		throws IOException, ClassNotFoundException
	{
		list = new LinkedList();
		int numElements = in.readInt();
		for (int i = 0; i < numElements; i++) {
			int first = in.readInt();
			int last = in.readInt();
			list.add(new IntRange(first, last));
		}
	}
	
	/**	Checks to see if the list is valid.
	 *
	 *	<p>For debugging.
	 *
	 *	@return		True if list is valid.
	 */
	 
	boolean isValid () {
		int last = -1;
		for (Iterator it = list.iterator(); it.hasNext(); ) {
			IntRange range = (IntRange)it.next();
			if (range.first <= last) return false;
			if (range.first >= range.last) return false;
			last = range.last;
		}
		return true;
	}

}

/*
 * <p>
 * Copyright &copy; 2004-2011 Northwestern University.
 * </p>
 * <p>
 * This program is free software; you can redistribute it
 * and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * </p>
 * <p>
 * This program is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the GNU General Public License for more
 * details.
 * </p>
 * <p>
 * You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330,
 * Boston, MA 02111-1307 USA.
 * </p>
 */

