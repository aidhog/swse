package org.semanticweb.swse.cli;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.qp.RMIQueryConstants;
import org.semanticweb.swse.qp.master.MasterQuery;
import org.semanticweb.yars.nx.Node;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class BenchmarkRemoteLucene {
	private final static Logger _log = Logger.getLogger(BenchmarkRemoteLucene.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option luceneO = new Option("l", "lucene dir");
		luceneO.setArgs(1);
		luceneO.setRequired(true);
		options.addOption(luceneO);
		
		Option sparseO = new Option("sp", "sparse index");
		sparseO.setArgs(1);
		sparseO.setRequired(true);
		options.addOption(sparseO);
		
		Option quadO = new Option("i", "quad index");
		quadO.setArgs(1);
		quadO.setRequired(true);
		options.addOption(quadO);
		
		Option kwO = new Option("kw", "line delimited keyword queries");
		kwO.setArgs(1);
		kwO.setRequired(true);
		options.addOption(kwO);
		
		Option topksO = new Option("topks", "topk evaluations: e.g. '-topks 5 10 100 1000'");
		topksO.setArgs(Option.UNLIMITED_VALUES);
		topksO.setRequired(true);
		options.addOption(topksO);
		
		Option srvsO = new Option("srvs", "servers file");
		srvsO.setArgs(1);
		srvsO.setRequired(true);
		options.addOption(srvsO);
		
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
		
		String kw = cmd.getOptionValue(kwO.getOpt());
		String lucene = cmd.getOptionValue(luceneO.getOpt());
		String sparse = cmd.getOptionValue(sparseO.getOpt());
		String quad = cmd.getOptionValue(quadO.getOpt());
		String servers = cmd.getOptionValue(srvsO.getOpt());
		
		String[] topkS = cmd.getOptionValues(topksO.getOpt());
		int [] topks = new int[topkS.length];
		for(int i=0; i<topkS.length; i++)
			topks[i] = Integer.parseInt(topkS[i]);
		
		benchmarkRemoteLucene(quad, sparse, lucene, servers, kw, topks);
	}

	public static void benchmarkRemoteLucene(String quad, String sparse, String lucene, String servers, String kw, int[] topks) throws Exception {
		File f = new File(servers);
		RMIRegistries rs = new RMIRegistries(f, RMIQueryConstants.DEFAULT_RMI_PORT);
		MasterQuery mq = new MasterQuery(rs, lucene, quad, sparse);
		
		ArrayList<String> kws = BenchmarkLocalLucene.getKeywords(kw);
		_log.info("Benchmarking using "+kws.size()+" keywords");
		
		for(int topk:topks){
			runEvaluation(mq, kws, topk);
		}
	}
	
	public static void runEvaluation(MasterQuery mq, ArrayList<String> kws, int topk) throws Exception{
		for(String kw:kws){
			long b4Time = System.currentTimeMillis();
			Iterator<Node[]> results = mq.keywordQuery(kw, 0, topk, null);
			long lookupTime = System.currentTimeMillis();
			
			_log.info("Looked up keyword "+kw+" in "+(lookupTime-b4Time)+" ms.");
			
			int c = 0;
			while(results.hasNext()){
				results.next();
				c++;
			}
			long iterTime = System.currentTimeMillis();
			_log.info("Scanned "+c+" result quads for keyword '"+kw+"' in "+(iterTime-lookupTime)+" ms.");
			
			if(!kw.equals(BenchmarkLocalLucene.WARMUP)){
				_log.info("Result (kw quads total-time lookup-time iter-time) :\t"+kw+"\t"+c+"\t"+(iterTime-b4Time)+"\t"+(lookupTime-b4Time)+"\t"+(iterTime-lookupTime));
			}
		}
	}
}
