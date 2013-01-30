package fr.liglab.lcm.internals;

import java.util.ArrayList;

/**
 * Dummy inheritance will ease future changes of transactions' actual representation
 */
public class Transactions extends ArrayList<Itemset> {
	
	public Transactions() {
		super();
	}
	
	public Transactions(int initialCapacity) {
		super(initialCapacity);
	}

	// avoid a compilation warning...
	private static final long serialVersionUID = 123467L;

}
