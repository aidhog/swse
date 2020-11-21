package org.semanticweb.swse.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.semanticweb.swse.lucene.utils.LuceneIndexBuilder;
import org.semanticweb.swse.qp.utils.QueryProcessor.KeywordResults;
import org.semanticweb.yars.nx.parser.ParseException;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class BenchmarkLocalLucene {
	private final static Logger _log = Logger.getLogger(BenchmarkLocalLucene.class.getSimpleName());
	
	protected final static String WARMUP = "warmup";
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option luceneO = new Option("l", "lucene dir");
		luceneO.setArgs(1);
		luceneO.setRequired(true);
		options.addOption(luceneO);
		
		Option kwO = new Option("kw", "line delimited keyword queries");
		kwO.setArgs(1);
		kwO.setRequired(true);
		options.addOption(kwO);
		
		Option topksO = new Option("topks", "topk evaluations: e.g. '-topks 5 10 100 1000'");
		topksO.setArgs(Option.UNLIMITED_VALUES);
		topksO.setRequired(true);
		options.addOption(topksO);
		
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
		
		String[] topkS = cmd.getOptionValues(topksO.getOpt());
		int [] topks = new int[topkS.length];
		for(int i=0; i<topkS.length; i++)
			topks[i] = Integer.parseInt(topkS[i]);
		
		benchmarkLocalLucene(lucene, kw, topks);
	}

	public static void benchmarkLocalLucene(String lucene, String kw, int[] topks) throws Exception {
		ArrayList<String> kws = getKeywords(kw);
		_log.info("Benchmarking using "+kws.size()+" keywords");
		
		NIOFSDirectory dir = new NIOFSDirectory(new File(lucene));
		IndexSearcher searcher = new IndexSearcher(dir, true);
		
		for(int topk:topks){
			runEvaluation(kws, searcher, topk);
		}
		
		dir.close();
	}
	
	public static void runEvaluation(ArrayList<String> kws, IndexSearcher searcher, int topk) throws IOException, org.apache.lucene.queryParser.ParseException, ParseException{
		for(String kw:kws){
			long b4Time = System.currentTimeMillis();
			Analyzer sa = new StandardAnalyzer(Version.LUCENE_CURRENT);
	
			QueryParser qp = new MultiFieldQueryParser(
					Version.LUCENE_CURRENT, 
					new String[] { 
							LuceneIndexBuilder.DocumentRepresentation.KEYWORDS,
							LuceneIndexBuilder.DocumentRepresentation.LABEL_TEXT
					}, 
					sa);
	
			qp.setDefaultOperator(QueryParser.Operator.AND);
	
			Query query = qp.parse(kw);
	
			ScoreDoc[] hits = searcher.search(query, topk).scoreDocs;
			
			long hitsTime = System.currentTimeMillis();
			
			_log.info("Got "+hits.length+" hits for keyword '"+kw+"' in "+(hitsTime-b4Time)+" ms.");
			
			if(hits.length!=topk){
				_log.info("Only got "+hits.length+" results for topk:"+topk+" kw:'"+kw+"'. Skipping...");
			} else{
				KeywordResults kwrs = new KeywordResults(hits, searcher, null);
				int c = 0;
				while(kwrs.hasNext()){
					kwrs.next();
					c++;
				}
				
				long snippetsTime = System.currentTimeMillis();
				
				_log.info("Got "+c+" snippet quads for '"+kw+"' in "+(snippetsTime-hitsTime)+" ms.");
				if(!kw.equals(WARMUP))
					_log.info("Result (kw topk snippet-quads total-time hits-time snippet-time) :\t"+kw+"\t"+topk+"\t"+c+"\t"+(snippetsTime-b4Time)+"\t"+(hitsTime-b4Time)+"\t"+(snippetsTime-hitsTime));
			}
		}
	}
	
	public static ArrayList<String> getKeywords(String kw) throws IOException{
		return getKeywords(kw, true);
	}
	
	public static ArrayList<String> getKeywords(String kw, boolean addWarmup) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(kw));
		ArrayList<String> kws = new ArrayList<String>();
		if(WARMUP!=null && addWarmup){
			_log.info("Using warmup query: '"+WARMUP+"'.");
			kws.add(WARMUP);
			
		}
		String line = null;
		while((line = br.readLine())!=null){
			line = line.trim();
			if(!line.isEmpty()){
				kws.add(URLDecoder.decode(line,"utf-8"));
			}
		}
		return kws;
	}
}
