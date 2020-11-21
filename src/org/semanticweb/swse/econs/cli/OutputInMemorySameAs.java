package org.semanticweb.swse.econs.cli;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.cons.utils.SameAsIndex;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.util.CallbackNxOutputStream;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class OutputInMemorySameAs {
	private final static Logger _log = Logger.getLogger(OutputInMemorySameAs.class.getSimpleName());

	public static final int TOP_K = 5;
	public static final int RANDOM_K = 100;
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();

		Option inO = new Option("in", "local nx.gz same as (s1,s2) file");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);

		Option outO = new Option("out", "local output nq.gz file");
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
		
		String in = cmd.getOptionValue("in");
		String out = cmd.getOptionValue("out");

		InputStream sis = null;
		sis = new FileInputStream(in);
		sis = new GZIPInputStream(sis);
		Iterator<Node[]> siter = new NxParser(sis);
		
		OutputStream os = new GZIPOutputStream(new FileOutputStream(out));
		CallbackNxOutputStream cb = new CallbackNxOutputStream(os);
		
		SameAsIndex sai = new SameAsIndex();

		int count = 0;

		long b4 = System.currentTimeMillis();
		_log.info("Reading same as");
		while(siter.hasNext()){
			Node[] next = siter.next();
			sai.addSameAs(next[0], next[1]);
			count++;
		}
		_log.info("...exhausted iterator in "+(System.currentTimeMillis()-b4)+" ms. ... read "+count+".");

		_log.info("...writing...");
		
		sai.writeSameAs(cb);
		
		os.close();
		sis.close();
		
		_log.info("...finished sameas write in "+(System.currentTimeMillis()-b4)+" ms.");
	}
}
