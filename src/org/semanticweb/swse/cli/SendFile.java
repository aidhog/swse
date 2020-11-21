package org.semanticweb.swse.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.saorr.auth.redirs.FileRedirects;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.file.RMIFileConstants;
import org.semanticweb.swse.file.SlaveFileArgs;
import org.semanticweb.swse.file.master.MasterFileArgs;
import org.semanticweb.yars.nx.parser.ParseException;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class SendFile {
	private final static Logger _log = Logger.getLogger(SendFile.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inO = new Option("in", "local file");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option outO = new Option("out", "remote file, can contain %");
		outO.setArgs(1);
		outO.setRequired(true);
		options.addOption(outO);
		
		Option serversO = new Option("srvs", "servers.dat file");
		serversO.setArgs(1);
		serversO.setRequired(true);
		options.addOption(serversO);
		
		Option rosO = new Option("ros", "use remote outputstream instead of remote inputstream");
		rosO.setArgs(0);
		rosO.setRequired(false);
		options.addOption(rosO);
		
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
		
		String in = cmd.getOptionValue("in");
		String out = cmd.getOptionValue("out");
		
		boolean ros = cmd.hasOption("ros");
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIFileConstants.DEFAULT_RMI_PORT);
		
		sendFile(in, servers, out, ros);
	}

	public static void sendFile(String in, RMIRegistries servers, String out, boolean useROS) throws Exception {
		SlaveFileArgs sfa = new SlaveFileArgs();
		
		MasterFileArgs mfa = new MasterFileArgs(sfa, in, out);
		mfa.setUseROutputStream(useROS);
		
		mfa.getTaskMaster().startRemoteTask(servers, RMIFileConstants.DEFAULT_STUB_NAME, mfa);
	}
}
