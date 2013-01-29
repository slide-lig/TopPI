package fr.liglab.lcm.internals;

import java.util.ArrayList;

/**
 * Dummy inheritance will ease future changes of transactions' actual representation
 */
public class Transactions extends ArrayList<Itemset> {

	// avoid a compilation warning...
	private static final long serialVersionUID = 123467L;

}
