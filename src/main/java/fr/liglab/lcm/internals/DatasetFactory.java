package fr.liglab.lcm.internals;

public final class DatasetFactory {
	
	
	
	/**
	 * Thrown when you shouldn't try to work on an extension
	 */
	public static class DontExploreThisBranchException extends Exception {
	        private static final long serialVersionUID = 2969583589161047791L;
	
	        public final int firstParent;
	        public final int extension;
	
	        /**
	         * @param extension the tested extension
	         * @param foundFirstParent
	         *            a item found in closure > extension
	         */
	        public DontExploreThisBranchException(int exploredExtension, int foundFirstParent) {
	                this.firstParent = foundFirstParent;
	                this.extension = exploredExtension;
	        }
	}
}
