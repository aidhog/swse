package org.semanticweb.swse.cli;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
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
public class BenchmarkRemoteQuadIndex {
	private final static Logger _log = Logger.getLogger(BenchmarkRemoteQuadIndex.class.getSimpleName());
	
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
		
		Option cacheO = new Option("c", "use cache");
		cacheO.setArgs(0);
		options.addOption(cacheO);
		
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
		
		boolean cache = cmd.hasOption(cacheO.getOpt());
		
		String[] topkS = cmd.getOptionValues(topksO.getOpt());
		int [] topks = new int[topkS.length];
		for(int i=0; i<topkS.length; i++)
			topks[i] = Integer.parseInt(topkS[i]);
		
		benchmarkRemoteQuadIndex(quad, sparse, lucene, servers, kw, topks, cache);
	}

	public static void benchmarkRemoteQuadIndex(String quad, String sparse, String lucene, String servers, String kw, int[] topks, boolean cache) throws Exception {
		File f = new File(servers);
		RMIRegistries rs = new RMIRegistries(f, RMIQueryConstants.DEFAULT_RMI_PORT);
		
		MasterQuery mq = null;
		
		if(cache) mq = new MasterQuery(rs, lucene, quad, sparse);
		else mq = new MasterQuery(rs, lucene, quad, sparse, -1, -1, -1);
		
		ArrayList<String> kws = BenchmarkLocalLucene.getKeywords(kw);
		_log.info("Benchmarking using "+kws.size()+" keywords");
		
		for(int topk:topks){
			runEvaluation(mq, kws, topk);
		}
	}
	
	public static void runEvaluation(MasterQuery mq, ArrayList<String> kws, int topk) throws Exception{
		for(String kw:kws){
			Iterator<Node[]> results = mq.keywordQuery(kw, 0, topk, null);
			
			
			TreeSet<Node> subjs = new TreeSet<Node>();
			while(results.hasNext()){
				subjs.add(results.next()[0]);
			}
			_log.info("Found "+subjs.size()+" results for keyword '"+kw+"'");
			
			
			for(Node sub:subjs){
				_log.info("Getting quads for "+sub.toN3()+".");
				long b4Time = System.currentTimeMillis();
				
				Iterator<Node[]> resultsF = mq.focus(sub, null);
				
				long lookupTime = System.currentTimeMillis();
				_log.info("Looked up key "+sub.toN3()+" in "+(lookupTime-b4Time)+" ms.");
				
				int c = 0;
				TreeSet<Node> entities = new TreeSet<Node>();
				while(resultsF.hasNext()){
					Node[] next = resultsF.next();
					entities.add(next[0]);
					c++;
				}
				long iterTime = System.currentTimeMillis();
				
				_log.info("Scanned "+c+" result quads for "+sub.toN3()+" in "+(iterTime-lookupTime)+" ms.");
				if(!kw.equals(BenchmarkLocalLucene.WARMUP)){
					_log.info("Result (node quads entities total-time lookup-time iter-time) :\t"+sub+"\t"+c+"\t"+entities.size()+"\t"+(iterTime-b4Time)+"\t"+(lookupTime-b4Time)+"\t"+(iterTime-lookupTime));
				}
			}
		}
	}
}
