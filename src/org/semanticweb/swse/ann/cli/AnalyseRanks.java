package org.semanticweb.swse.ann.cli;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.rmi.AlreadyBoundException;
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
public class AnalyseRanks {
	
	static Logger _log = Logger.getLogger(AnalyseRanks.class.getName());
	
	public static final int TICKS = 10000000;
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, AlreadyBoundException, org.semanticweb.yars.nx.parser.ParseException{
		Option inputO = new Option("i", "input");
		inputO.setArgs(1);
		inputO.setRequired(true);
		
		Option outputO = new Option("o", "output rank distrib");
		outputO.setArgs(1);
		
		Option posO = new Option("n", "position of rank element");
		posO.setArgs(1);
		posO.setRequired(true);
		
		Option gzO = new Option("gz", "gz");
		gzO.setArgs(0);
		
		Option helpO = new Option("h", "print help");
				
		Options options = new Options();
		options.addOption(inputO);
		options.addOption(posO);
		options.addOption(gzO);
		options.addOption(outputO);
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

		String input = cmd.getOptionValue("i");
		String output = cmd.getOptionValue("o");
		
		boolean gz = false;
		if (cmd.hasOption("gz")) {
			gz = true;
		}
		
		int pos = Integer.parseInt(cmd.getOptionValue("n"));
		
		analyseRanks(input, gz, pos, output);
	}
	
	public static void analyseRanks(String file, boolean gz, int pos) throws IOException, org.semanticweb.yars.nx.parser.ParseException{
		analyseRanks(file, gz, pos, null);
	}
	
	public static void analyseRanks(String file, boolean gz, int pos, String out) throws IOException, org.semanticweb.yars.nx.parser.ParseException{
		InputStream is = new FileInputStream(file);
		if(gz){
			is = new GZIPInputStream(is);
		}
		
		NxParser nxp = new NxParser(is);
		
		
		Count<Double> ranks = new Count<Double>();
		double summation = 0;
		double count = 0;
		Node[] max = null;
		double maxRank = Double.MIN_VALUE;
		while(nxp.hasNext()){
			Node[] next = nxp.next();
			double r = Double.parseDouble(next[pos].toString());
			count++;
			if(count%TICKS==0){
				_log.info("Done "+count);
			}
			ranks.add(r);
			summation+=r;
			
			if(r>maxRank){
				maxRank = r;
				max = next;
			}
		}
		
		_log.info("Read "+count+" tuples");
		_log.info("Rank sum "+summation);
		_log.info("Rank average "+summation/(double)count);
		_log.info("Max rank "+maxRank);
		_log.info("(First) max tuple "+Nodes.toN3(max));
		_log.info("Distrib...");
		ranks.printOrderedStats(_log, Level.INFO);

		if(out!=null){
			PrintStream ps = new PrintStream(new FileOutputStream(out));
			ranks.printOrderedStats(ps);
			ps.close();
		}
		
		is.close();
	}
}