package fr.liglab.lcm;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import fr.liglab.lcm.mapred.Driver;

/**
 * Hadoop program entry
 * 
 * @author Martin Kirchgessner
 */
public class Main {

	private static final String OPTION_TOPK = "k";
	private static final String OPTION_GROUPS = "g";

	public static Options getOptions() {
		Options options = new Options();

		options.addOption("h", false, "Show help");
		options.addOption(Main.OPTION_GROUPS, true,
				"(only for map-reduce) Number of groups in which frequent items are dispatched (defaults to 50)");
		options.addOption(Main.OPTION_TOPK, true, "Run in top-k-per-item mode");

		return options;
	}

	public static void main(String[] args) {

		Options options = getOptions();
		CommandLineParser parser = new PosixParser();

		try {
			Configuration conf = new Configuration();
			GenericOptionsParser hadoopCmd = new GenericOptionsParser(conf, args);
			CommandLine cmd = parser.parse(options, hadoopCmd.getRemainingArgs());
			String[] remainingArgs = cmd.getArgs();

			if (remainingArgs.length == 3) {

				String input = remainingArgs[0];
				int minSupport = Integer.parseInt(remainingArgs[1]);

				String output = null;
				if (remainingArgs.length == 3) {
					output = remainingArgs[2];
				}

				Integer k = null;
				if (cmd.hasOption(OPTION_TOPK)) {
					k = Integer.parseInt(cmd.getOptionValue(OPTION_TOPK));

					if (k <= 0) {
						throw new RuntimeException("When provided, K must be positive");
					}
				}
				
				Integer g = 50;
				if (cmd.hasOption(OPTION_GROUPS)) {
					g = Integer.parseInt(cmd.getOptionValue(OPTION_GROUPS));

					if (g <= 0) {
						throw new RuntimeException("When provided, G must be positive");
					}
				}

				conf = hadoopCmd.getConfiguration();

				conf.setStrings(Driver.KEY_INPUT, input);
				conf.setStrings(Driver.KEY_OUTPUT, output);
				conf.setInt(Driver.KEY_MINSUP, minSupport);
				conf.setInt(Driver.KEY_NBGROUPS, g);

				if (k != null) {
					conf.setInt(Driver.KEY_DO_TOP_K, k);
				}

				Driver driver = new Driver(conf);
				System.exit(driver.run());
				
			} else {
				printMan(options);
			}
		} catch (ParseException e) {
			System.out.println(Arrays.toString(args));
			printMan(options);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void printMan(Options options) {
		String syntax = "hadoop jar [path/to/lcm.jar] [GENERIC_OPTIONS] [OPTIONS] INPUT_PATH MINSUP OUTPUT_PATH";
		String header = "\nOUTPUT_PATH is optional in standalone mode. When missing, patterns are printed to standard output.\n\nOptions are :";
		String footer = "\n\nTweaking properties : \n" + Driver.KEY_GROUPER_CLASS + "\n" + 
		Driver.KEY_SINGLE_GROUP_ID + "\n" + Driver.KEY_DUMP_ON_HEAP_EXN + "\n" + 
				Driver.KEY_NB_THREADS + "\n" + Driver.KEY_LONG_TRANSACTION_MODE_THRESHOLD + "\n" +
				Driver.KEY_GEN_DBS_ONLY;

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(syntax, header, options, footer);

		GenericOptionsParser.printGenericCommandUsage(System.out);
	}
}
