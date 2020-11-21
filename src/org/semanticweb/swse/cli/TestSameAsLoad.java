package org.semanticweb.swse.cli;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.cons.utils.SameAsIndex;
import org.semanticweb.swse.cons.utils.SameAsIndex.SameAsList;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class TestSameAsLoad {
	private final static Logger _log = Logger.getLogger(TestSameAsLoad.class.getSimpleName());

	public static final int TOP_K = 5;
	public static final int RANDOM_K = 100;
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();

		Option inO = new Option("in", "local nxz same as (s1,s2) file");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);

		Option ranksO = new Option("ranks", "local ranks nq.gz file");
		ranksO.setArgs(1);
		options.addOption(ranksO);

		Option helpO = new Option("h", "print help");
		options.addOption(helpO);
		
		Option tO = new Option("t", "record top t class sizes");
		tO.setArgs(1);
		options.addOption(tO);
		
		Option rO = new Option("r", "record r random classes");
		rO.setArgs(1);
		options.addOption(rO);

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
		
		int tk = TOP_K, rk = RANDOM_K;
		if(cmd.hasOption("t")){
			tk = Integer.parseInt(cmd.getOptionValue("t"));
		}
		if(cmd.hasOption("r")){
			rk = Integer.parseInt(cmd.getOptionValue("r"));
		}

		String in = cmd.getOptionValue("in");
		String ranks = cmd.getOptionValue("ranks");

		InputStream ris = null;
		Iterator<Node[]> riter = null;
		if(ranks!=null){
			ris = new FileInputStream(ranks);
			ris = new GZIPInputStream(ris);
			riter = new NxParser(ris);
		}

		InputStream sis = null;
		sis = new FileInputStream(in);
		sis = new GZIPInputStream(sis);
		Iterator<Node[]> siter = new NxParser(sis);

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

		HashSet<Node> rs = new HashSet<Node>();
		_log.info("...reading ranks...");
		count = 0;
		if(riter!=null) while(riter.hasNext()){
			Node[] next = riter.next();
			sai.setRank(next);
			count++;
			if(count<tk){
				rs.add(next[0]);
			}
		}
		_log.info("...read "+count+" ranks...");
		_log.info("...finished loading in "+(System.currentTimeMillis()-b4)+" ms.");
		
		Runtime.getRuntime().gc();
		System.err.println("Max memory "+Runtime.getRuntime().maxMemory());
		System.err.println("Used memory "+Runtime.getRuntime().totalMemory());
		System.err.println("Free memory "+Runtime.getRuntime().freeMemory());

		sai.logStats(tk, rk);
		
		for(Node r:rs){
			SameAsList sal = sai.getSameAsList(r);
			if(sal==null)
				_log.info("Ranked EQC Ranked "+r+" EQCS Size 0 EQCS ");
			else
				_log.info("Ranked EQC Ranked "+r+" EQCS Size "+sal.size()+" EQCS "+sal);
		}
		
		_log.info("...finished benchmark in "+(System.currentTimeMillis()-b4)+" ms.");
	}
}
