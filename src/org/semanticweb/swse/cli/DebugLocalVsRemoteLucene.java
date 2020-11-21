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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.lucene.utils.LuceneIndexBuilder;
import org.semanticweb.swse.qp.RMIQueryConstants;
import org.semanticweb.swse.qp.master.MasterQuery;
import org.semanticweb.swse.qp.utils.QueryProcessor.KeywordResults;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class DebugLocalVsRemoteLucene {
	private final static Logger _log = Logger.getLogger(DebugLocalVsRemoteLucene.class.getSimpleName());

	public static void main(String args[]) throws Exception{
		Options options = new Options();

		Option lluceneO = new Option("ll", "local lucene dir");
		lluceneO.setArgs(1);
		lluceneO.setRequired(true);
		options.addOption(lluceneO);

		Option rluceneO = new Option("rl", "remote lucene dir");
		rluceneO.setArgs(1);
		rluceneO.setRequired(true);
		options.addOption(rluceneO);

		Option sparseO = new Option("sp", "remote sparse index");
		sparseO.setArgs(1);
		sparseO.setRequired(true);
		options.addOption(sparseO);

		Option quadO = new Option("i", "remote quad index");
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
		String llucene = cmd.getOptionValue(lluceneO.getOpt());
		String rlucene = cmd.getOptionValue(rluceneO.getOpt());
		String sparse = cmd.getOptionValue(sparseO.getOpt());
		String quad = cmd.getOptionValue(quadO.getOpt());
		String servers = cmd.getOptionValue(srvsO.getOpt());

		String[] topkS = cmd.getOptionValues(topksO.getOpt());
		int [] topks = new int[topkS.length];
		for(int i=0; i<topkS.length; i++)
			topks[i] = Integer.parseInt(topkS[i]);

		benchmarkLocalVsRemoteLucene(rlucene, quad, sparse, llucene, servers, kw, topks);
	}

	public static void benchmarkLocalVsRemoteLucene(String rlucene, String quad, String sparse, String llucene, String servers, String kw, int[] topks) throws Exception {
		File f = new File(servers);
		RMIRegistries rs = new RMIRegistries(f, RMIQueryConstants.DEFAULT_RMI_PORT);
		MasterQuery mq = new MasterQuery(rs, rlucene, quad, sparse);

		NIOFSDirectory dir = new NIOFSDirectory(new File(llucene));
		IndexSearcher searcher = new IndexSearcher(dir, true);

		ArrayList<String> kws = BenchmarkLocalLucene.getKeywords(kw);
		_log.info("Debugging using "+kws.size()+" keywords");

		for(int topk:topks){
			runEvaluation(mq, kws, searcher, topk);
		}
	}

	public static void runEvaluation(MasterQuery mq, ArrayList<String> kws, IndexSearcher searcher, int topk) throws Exception{
		int okay = 0, localm = 0, remotem = 0;
		for(String kw:kws){
			Iterator<Node[]> results = mq.keywordQuery(kw, 0, topk, null);
			TreeSet<Node[]> remote = new TreeSet<Node[]>(NodeComparator.NC);

			TreeSet<Node> remoteS = new TreeSet<Node>();
			
			while(results.hasNext()){
				Node[] next = results.next();
				remote.add(next);
				remoteS.add(next[0]);
			}

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


			KeywordResults kwrs = new KeywordResults(hits, searcher, null);
			TreeSet<Node[]> local = new TreeSet<Node[]>(NodeComparator.NC);
			
			
			TreeSet<Node> localS = new TreeSet<Node>();
			
			while(kwrs.hasNext()){
				Node[] next = kwrs.next();
				local.add(next);
				localS.add(next[0]);
			}
			
			_log.info("For for '"+kw+"' Local results "+local.size()+" Remote results "+remote.size()+" Local subjects "+localS.size()+" Remote subjects "+remoteS.size());
			
			TreeSet<Node[]> all = new TreeSet<Node[]>(NodeComparator.NC);
			all.addAll(local);
			all.addAll(remote);
			
			boolean rokay = true;
			boolean lokay = true;
			for(Node[] a:all){
				if(!remote.contains(a)){
					rokay = false;
					_log.info("Remote results for '"+kw+"' missing quad: "+Nodes.toN3(a));
				} else if(!local.contains(a)){
					lokay = false;
					_log.info("Local results for '"+kw+"' missing quad: "+Nodes.toN3(a));
				}
			}

			if(rokay && lokay){
				_log.info("No problems for '"+kw+"'");
				okay++;
			}
			
			if(!rokay)remotem++;
			if(!lokay)localm++;
		}
		
		_log.info("Okay :"+okay);
		_log.info("Remote missing local results :"+remotem);
		_log.info("Local missing remote results :"+localm);
	}
}
