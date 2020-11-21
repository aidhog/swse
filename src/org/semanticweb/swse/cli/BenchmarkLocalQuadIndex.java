package org.semanticweb.swse.cli;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.semanticweb.nxindex.NodesIndex;
import org.semanticweb.nxindex.block.NodesBlockReaderNIO;
import org.semanticweb.nxindex.sparse.SparseIndex;
import org.semanticweb.swse.lucene.utils.LuceneIndexBuilder;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class BenchmarkLocalQuadIndex {
	private final static Logger _log = Logger.getLogger(BenchmarkLocalQuadIndex.class.getSimpleName());
	
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
		
		String[] topkS = cmd.getOptionValues(topksO.getOpt());
		int [] topks = new int[topkS.length];
		for(int i=0; i<topkS.length; i++)
			topks[i] = Integer.parseInt(topkS[i]);
		
		
		
		benchmarkLocalIndex(quad, sparse, lucene, kw, topks);
	}

	public static void benchmarkLocalIndex(String quad, String sparse, String lucene, String kw, int[] topks) throws Exception {
		NodesBlockReaderNIO nbrio = new NodesBlockReaderNIO(quad);
		SparseIndex sp = new SparseIndex(sparse);
		NodesIndex ni = new NodesIndex(nbrio, sp);
		
		ArrayList<String> kws = BenchmarkLocalLucene.getKeywords(kw);
		_log.info("Benchmarking using "+kws.size()+" keywords");
		
		NIOFSDirectory dir = new NIOFSDirectory(new File(lucene));
		IndexSearcher searcher = new IndexSearcher(dir, true);
		
		for(int topk:topks){
			runEvaluation(ni, kws, searcher, topk);
		}
		
		dir.close();
	}
	
	public static void runEvaluation(NodesIndex ni, ArrayList<String> kws, IndexSearcher searcher, int topk) throws IOException, org.apache.lucene.queryParser.ParseException, ParseException{
		for(String kw:kws){
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
			
			_log.info("Got "+hits.length+" hits for keyword '"+kw+"'");
			
			for(ScoreDoc hit:hits){
				Document d = searcher.doc(hit.doc);
				String sub = d.get(LuceneIndexBuilder.DocumentRepresentation.SUBJECT);
				Node n = NxParser.parseNode(sub);
				
				_log.info("Getting quads for "+sub+".");
				long b4Time = System.currentTimeMillis();
				Iterator<Node[]> lookupIter = ni.getIterator(new Node[]{n});
				long lookupTime = System.currentTimeMillis();
				
				_log.info("Looked up key "+sub+" in "+(lookupTime-b4Time)+" ms.");
				int c = 0;
				while(lookupIter.hasNext()){
					lookupIter.next();
					c++;
				}
				long iterTime = System.currentTimeMillis();
				_log.info("Scanned "+c+" result quads for "+n+" in "+(iterTime-lookupTime)+" ms.");
				if(!kw.equals(BenchmarkLocalLucene.WARMUP)){
					_log.info("Result (node quads total-time lookup-time iter-time) :\t"+sub+"\t"+c+"\t"+(iterTime-b4Time)+"\t"+(lookupTime-b4Time)+"\t"+(iterTime-lookupTime));
				}
			}
//			if(hits.length!=topk){
//				_log.info("Only got "+hits.length+" results for topk:"+topk+" kw:'"+kw+"'. Skipping...");
//			} else{
//				KeywordResults kwrs = new KeywordResults(hits, searcher, null);
//				int c = 0;
//				while(kwrs.hasNext()){
//					kwrs.next();
//					c++;
//				}
//				
//				long snippetsTime = System.currentTimeMillis();
//				
//				_log.info("Got "+c+" snippet quads for '"+kw+"' in "+(snippetsTime-hitsTime)+" ms.");
//				_log.info("Result (kw topk snippet-quads total-time hits-time snippet-time) :\t"+kw+"\t"+topk+"\t"+c+"\t"+(snippetsTime-b4Time)+"\t"+(hitsTime-b4Time)+"\t"+(snippetsTime-hitsTime));
//			}
		}
	}
}
