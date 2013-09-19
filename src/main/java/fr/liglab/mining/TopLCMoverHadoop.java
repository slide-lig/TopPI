package fr.liglab.mining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;

/**
 * The Hadoop driver.
 */
public class TopLCMoverHadoop implements Tool {
	////////////////////MANDATORY CONFIGURATION PROPERTIES ////////////////////
		
	public static final String KEY_INPUT    = "toplcm.path.input";
	public static final String KEY_OUTPUT   = "toplcm.path.output";
	public static final String KEY_MINSUP   = "toplcm.minsup";
	public static final String KEY_NBGROUPS = "toplcm.nbGroups";
	public static final String KEY_K        = "toplcm.topK";
	
	
	//////////////////// INTERNAL CONFIGURATION PROPERTIES ////////////////////
	
	/**
	 * this property will be filled after item counting
	 */
	public static final String KEY_REBASING_MAX_ID = "toplcm.items.maxId";
	
	
	
	private Configuration conf;
	private String input;
	private String outputPrefix;
	
	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void setConf(Configuration c) {
		this.conf = c;
		this.input = c.get(KEY_INPUT);
		this.outputPrefix = c.get(KEY_OUTPUT);
	}
	
	public TopLCMoverHadoop(Configuration c) {
		this.setConf(c);
	}
	
	/**
	 * @param args should contain INPUT FREQ_THRES K NB_GROUPS OUTPUT
	 */
	@Override
	public int run(String[] args) throws Exception {
		
		
		return 0;
	}

}
