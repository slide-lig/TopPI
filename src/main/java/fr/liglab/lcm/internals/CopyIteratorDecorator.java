package fr.liglab.lcm.internals;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * An Iterator wrapper able to return (after a first complete pass)
 * new iterators on the same data, in the same order
 */
public class CopyIteratorDecorator<T> implements Iterator<T> {
	
	private final Iterator<T> decorated;
	private final ArrayList<T> copied = new ArrayList<T>();
	
	public CopyIteratorDecorator(Iterator<T> source) {
		this.decorated = source;
	}
	
	/**
	 * you should have reached hasNext==false before
	 * @return how many items have been copied
	 */
	public int size() {
		return this.copied.size();
	}
	
	/**
	 * you should have reached hasNext==false before
	 * @return a new iterator on previously red data
	 */
	public Iterator<T> newIterator() {
		return this.copied.iterator();
	}
	
	public boolean hasNext() {
		return this.decorated.hasNext();
	}
	
	public T next() {
		T next = this.decorated.next();
		this.copied.add(next);
		return next;
	}
	
	public void remove() {
		this.decorated.remove();
		this.copied.remove(copied.size() - 1);
	}
}
