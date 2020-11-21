package org.semanticweb.swse.cli;

import java.io.File;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.index2.RMIIndexerConstants;
import org.semanticweb.swse.index2.SlaveIndexerArgs;
import org.semanticweb.swse.index2.master.MasterIndexer;
import org.semanticweb.swse.index2.master.MasterIndexerArgs;

/**
 * Main method to conduct distributed indexing using remote indexers 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteIndexing2 {
	private final static Logger _log = Logger.getLogger(RunRemoteIndexing2.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option linO = new Option("lin", "local input files, can use % delimiter");
		linO.setArgs(Option.UNLIMITED_VALUES);
		linO.setValueSeparator(',');
		options.addOption(linO);
		
		Option rinO = new Option("rin", "remote input files, can use % delimiter (individual files delimited by ',')");
		rinO.setArgs(Option.UNLIMITED_VALUES);
		rinO.setValueSeparator(',');
		options.addOption(rinO);
		
		Option gzlinO = new Option("gzlin", "flags stating which local input files are gzipped, e.g. -gzlin 0,2,3");
		gzlinO.setArgs(Option.UNLIMITED_VALUES);
		gzlinO.setValueSeparator(',');
		options.addOption(gzlinO);
		
		Option gzrinO = new Option("gzrin", "flags stating which remote input files are gzipped, e.g. -gzrin 0,2,3");
		gzrinO.setArgs(Option.UNLIMITED_VALUES);
		gzrinO.setValueSeparator(',');
		options.addOption(gzrinO);
		
		Option serversO = new Option("srvs", "servers.dat file");
		serversO.setArgs(1);
		serversO.setRequired(true);
		options.addOption(serversO);
		
		Option outO = new Option("out", "remote/local output dir, can use a % delimiter");
		outO.setArgs(1);
		outO.setRequired(true);
		options.addOption(outO);
		
		Option helpO = new Option("h", "print help");
		options.addOption(helpO);

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e) {
			System.err.println("***ERROR: " + e.getClass() + ": " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		if (cmd.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		
		String[] rin = cmd.getOptionValues("rin");
		String[] lin = cmd.getOptionValues("lin");
		
		boolean[] gzrin = null;
		if(rin!=null)gzrin = getBooleanArray(cmd.getOptionValues("gzrin"), rin.length);
		boolean[] gzlin = null;
		if(lin!=null)gzlin = getBooleanArray(cmd.getOptionValues("gzlin"), lin.length);
		
		String out = cmd.getOptionValue("out");
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIIndexerConstants.DEFAULT_RMI_PORT);
		
		runRemoteIndexing(lin, gzlin, rin, gzrin, servers, out);
	}
	
	public static void runRemoteIndexing(String[] lin, boolean[] gzlin,
			String[] rin, boolean[] gzrin, RMIRegistries servers, String outdir) throws Exception {
		SlaveIndexerArgs sia = new SlaveIndexerArgs(SlaveIndexerArgs.getDefaultScatterDir(outdir),
				SlaveIndexerArgs.getDefaultGatherDir(outdir),
				SlaveIndexerArgs.getDefaultIndexFile(outdir),
				SlaveIndexerArgs.getDefaultSparseFile(outdir));
		
		MasterIndexerArgs mia = new MasterIndexerArgs(lin, rin, MasterIndexerArgs.getDefaultScatterDir(outdir), sia);
		mia.setGzLocal(gzlin);
		mia.setGzRemote(gzrin);
		
		MasterIndexer mi = new MasterIndexer();
		mi.startRemoteTask(servers, RMIIndexerConstants.DEFAULT_STUB_NAME ,mia);
	}

	private static boolean[] getBooleanArray(String[] args, int files){
		boolean[] ba = new boolean[files];
		if(args==null){
			return ba;
		}
		for(String s:args){
			ba[Integer.parseInt(s)] = true;
		}
		return ba;
	}
}
