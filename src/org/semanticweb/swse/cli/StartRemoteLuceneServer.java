package org.semanticweb.swse.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.AlreadyBoundException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.lucene.RMILuceneConstants;
import org.semanticweb.swse.lucene.RMILuceneServer;

/**
 * Main method to setup a reasoning service which can be run via RMI.
 * 
 * @author aidhog
 */
public class StartRemoteLuceneServer {
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, AlreadyBoundException{
		long time = System.currentTimeMillis();
		
		Option hostnameO = new Option("n", "hostname (defaults to localhost");
		hostnameO.setArgs(1);
		
		Option registryO = new Option("r", "start the RMI registry");
		registryO.setArgs(0);

		Option portO = new Option("p", "RMI port");
		portO.setArgs(1);

		Option helpO = new Option("h", "print help");
				
		Options options = new Options();
		options.addOption(hostnameO);
		options.addOption(portO);
		options.addOption(registryO);
		options.addOption(helpO);

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
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

		String hostname = null;
		if (cmd.hasOption("n")) {
			hostname = cmd.getOptionValue("n");
		}
		int port = RMILuceneConstants.DEFAULT_RMI_PORT;
		if (cmd.hasOption("p")) {
			port = Integer.parseInt(cmd.getOptionValue("p"));
		}
		boolean startReg = false;
		if (cmd.hasOption("r")) {
			startReg = true;
		}
		
		if(startReg){
			RMIUtils.startRMIRegistry(port);
			System.err.println("Registry setup on port " + port);
		}
		
		RMILuceneServer.startRMIServer(hostname, port, RMILuceneConstants.DEFAULT_STUB_NAME);
		
		long time1 = System.currentTimeMillis();
	    
	    System.err.println("Server ready in " + (time1-time) + " ms.");
	    
	    if(startReg){
	    	System.err.println("Keeping alive (registry)... Enter k to kill:");
	    	BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	    	String line;
	    	while((line = br.readLine())!=null && !line.trim().equals("k")){
	    		System.err.println("Type k to kill: "+line);
	    	}
	    }
	}
}