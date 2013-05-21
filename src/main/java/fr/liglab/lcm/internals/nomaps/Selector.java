package fr.liglab.lcm.internals.nomaps;

/**
 * Main class for chained exploration filters, implemented as an immutable chained list.
 */
public abstract class Selector {
	
	private final Selector next;
	
	/**
	 * @param extension
	 * @param state
	 * @return false if, at the given state, trying to extend the current pattern with the given extension is useless
	 * @throws WrongFirstParentException 
	 */
	abstract protected boolean allowExploration(int extension, ExplorationStep state)
		throws WrongFirstParentException;
	
	/**
	 * @return an instance of the same selector for another recursion
	 */
	abstract protected Selector copy(Selector newNext);
	
	public Selector() {
		this.next = null;
	}
	
	protected Selector(Selector follower) {
		this.next = follower;
	}
	
	/**
	 * This one handles chained calls
	 * @param extension
	 * @param state
	 * @return false if, at the given state, trying to extend the current pattern with the given extension is useless
	 * @throws WrongFirstParentException 
	 */
	final boolean select(int extension, ExplorationStep state) throws WrongFirstParentException {
		
		return this.allowExploration(extension, state) && 
				(this.next == null || this.next.select(extension, state));
	}
	
	/**
	 * @return a new Selector chain for a new recursion
	 */
	final Selector copy() {
		return this.append(null);
	}
	
	/**
	 * Note: prepending should simply be done by passing a chain at first selector's instantiation.
	 * Appends the given selector at the end of current list, and returns new list's head
	 */
	final Selector append(Selector s) {
		if (this.next == null) {
			return this.copy(s);
		} else {
			return this.copy(this.next.append(s));
		}
	}
	
	/**
	 * Thrown when a Selector finds that an extension won't be the first parent of its closed pattern
	 * (FirstParentTest should be the only one concerned)
	 */
	public static class WrongFirstParentException extends Exception {
        private static final long serialVersionUID = 2969583589161047791L;

        public final int firstParent;
        public final int extension;

        /**
         * @param extension the tested extension
         * @param foundFirstParent
         *            a item found in closure > extension
         */
        public WrongFirstParentException(int exploredExtension, int foundFirstParent) {
                this.firstParent = foundFirstParent;
                this.extension = exploredExtension;
        }
	}
}
