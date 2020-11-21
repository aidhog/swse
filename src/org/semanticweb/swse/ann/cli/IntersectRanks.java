package org.semanticweb.swse.ann.cli;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.rmi.AlreadyBoundException;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.stats.Count;


/**
 * Main method to setup a reasoning service which can be run via RMI.
 * 
 * @author aidhog
 */
public class IntersectRanks {
	
	static Logger _log = Logger.getLogger(IntersectRanks.class.getName());
	
	public static final int TICKS = 10000000;
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, AlreadyBoundException, org.semanticweb.yars.nx.parser.ParseException{
		Option input1O = new Option("i1", "raw ranks");
		input1O.setArgs(1);
		input1O.setRequired(true);
		
		Option input2O = new Option("i2", "target ranks");
		input2O.setArgs(1);
		input2O.setRequired(true);
		
		Option pos1O = new Option("n1", "position of raw rank element");
		pos1O.setArgs(1);
		pos1O.setRequired(true);
		
		Option pos2O = new Option("n2", "position of target rank element");
		pos2O.setArgs(1);
		pos2O.setRequired(true);
		
		Option gz1O = new Option("gz1", "raw gz");
		gz1O.setArgs(0);
		
		Option gz2O = new Option("gz2", "target gz");
		gz2O.setArgs(0);
		
		Option helpO = new Option("h", "print help");
				
		Options options = new Options();
		options.addOption(input1O);
		options.addOption(input2O);
		options.addOption(pos1O);
		options.addOption(pos2O);
		options.addOption(gz1O);
		options.addOption(gz2O);
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

		String input1 = cmd.getOptionValue("i1");
		String input2 = cmd.getOptionValue("i2");
		
		boolean gz1 = false;
		if (cmd.hasOption("gz1")) {
			gz1 = true;
		}
		
		boolean gz2 = false;
		if (cmd.hasOption("gz2")) {
			gz2 = true;
		}
		
		int pos1 = Integer.parseInt(cmd.getOptionValue("n1"));
		int pos2 = Integer.parseInt(cmd.getOptionValue("n2"));
		
		intersectRanks(input1, gz1, pos1, input2, gz2, pos2);
	}
	
	public static void intersectRanks(String raw, boolean gzraw, int posraw, String tgt, boolean gztgt, int postgt) throws IOException, org.semanticweb.yars.nx.parser.ParseException{
		HashSet<Double> targetRanks = new HashSet<Double>();
		
		InputStream tis = new FileInputStream(tgt);
		if(gztgt){
			tis = new GZIPInputStream(tis);
		}
		
		NxParser nxptgt = new NxParser(tis);
		
		int c = 0;
		while(nxptgt.hasNext()){
			c++;
			targetRanks.add(Double.parseDouble(nxptgt.next()[postgt].toString()));
		}
		
		_log.info("Read "+c+" target tuples...");
		
		_log.info("Found "+targetRanks.size()+" unique target ranks...");
		tis.close();
		
		InputStream ris = new FileInputStream(raw);
		if(gzraw){
			ris = new GZIPInputStream(ris);
		}
		
		NxParser nxpraw = new NxParser(ris);
		
		int r = 0, t = 0, nt = 0;
		while(nxpraw.hasNext()){
			r++;
			if(targetRanks.contains(Double.parseDouble(nxpraw.next()[posraw].toString()))){
				t++;
			} else{
				nt++;
			}
		}
		
		_log.info("Read "+c+" raw tuples...");
		
		_log.info("Found "+t+" ranks intersect...");
		_log.info("Found "+nt+" ranks don't intersect...");

		ris.close();
	}
}