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
import org.semanticweb.swse.econs.ercons.RMIConsolidationServer;
import org.semanticweb.swse.econs.ercons.utils.ConsolidationIterator.HandleNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.util.CallbackNxOutputStream;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunLocalConsolidation {
	private final static Logger _log = Logger.getLogger(RunLocalConsolidation.class.getSimpleName());

	public static void main(String args[]) throws Exception{
		NxParser.DEFAULT_PARSE_DTS = false;
		Options options = new Options();

		Option in1O = new Option("i1", "gz nx sorted data to consolidate");
		in1O.setArgs(1);
		in1O.setRequired(true);
		options.addOption(in1O);
		
		Option in2O = new Option("i2", "gz nx sorted closed sameas file s1>s2");
		in2O.setArgs(1);
		in2O.setRequired(true);
		options.addOption(in2O);

		Option outO = new Option("o", "out gz nx consolidated file");
		outO.setArgs(1);
		outO.setRequired(true);
		options.addOption(outO);
		
		Option posO = new Option("n", "pos sorted by/to consolidate");
		posO.setArgs(1);
		options.addOption(posO);
		
		Option srtO = new Option("srt", "don't rewrite rdf:type positions");
		srtO.setOptionalArg(true);
		options.addOption(srtO);

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
		String ranks = cmd.getOptionValue("ranks");
		
		InputStream is = new GZIPInputStream(new FileInputStream(cmd.getOptionValue("i1")));
		Iterator<Node[]> nxp = new NxParser(is);

		InputStream sais = new GZIPInputStream(new FileInputStream(cmd.getOptionValue("i2")));
		Iterator<Node[]> sanxp = new NxParser(sais);

		OutputStream os = new GZIPOutputStream(new FileOutputStream(cmd.getOptionValue("o")));
		Callback cb = new CallbackNxOutputStream(os);
		
		boolean skipRT = cmd.hasOption("srt");
		
		long b4 = System.currentTimeMillis();
		
		int pos = 0;
		if(cmd.hasOption("n"))
			pos = Integer.parseInt(cmd.getOptionValue("n"));
		
		_log.info("Position "+pos);

		if(skipRT){
			RMIConsolidationServer.rewrite(nxp, sanxp, cb, pos, HandleNode.BUFFER);
		} else{
			RMIConsolidationServer.rewrite(nxp, sanxp, cb, pos, HandleNode.REWRITE);
		}
		
		is.close();
		os.close();
		sais.close();
		
		_log.info("...finished consolidation in "+(System.currentTimeMillis()-b4)+" ms.");
	}
}
