package org.semanticweb.swse.cli;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.nxindex.ScanIterator;
import org.semanticweb.nxindex.block.NodesBlockReader;
import org.semanticweb.nxindex.block.NodesBlockReaderNIO;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.Namespace;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.stats.Count;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.ontologycentral.ldspider.tld.TldManager;

public class RankClassesPreds {
	static transient Logger _log = Logger.getLogger(RankClassesPreds.class.getName());
	
	static String RDF_CMP_PREFIX = "<"+RDF.NS+"_";
	static Resource RDF_CMP_ALL = new Resource(RDF.NS+"_all_cmp");
	
	static TldManager TLDM; 
	static { 
		try {
			TLDM= new TldManager();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} 

		Logger tldm_log = Logger.getLogger(TldManager.class.getName()); 
		if(tldm_log!=null)
			tldm_log.setLevel(Level.OFF);
	};
	
	final static String NO_NS = "no_ns";
	final static String NO_PLD = "no_pld";
	final static String STATS_OBJ = "cp_rank_stats.jo.gz";
	
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws org.semanticweb.yars.nx.parser.ParseException 
	 */
	public static void main(String[] args) throws org.semanticweb.yars.nx.parser.ParseException, IOException {
		Options	options = new Options();
		
		org.semanticweb.yars.nx.cli.Main.addTicksOption(options);
		
		Option inputO = new Option("i", "name of files to read");
		inputO.setArgs(Option.UNLIMITED_VALUES);
		inputO.setRequired(true);
		options.addOption(inputO);

		Option inputfO = new Option("if", "input format of *all* files, nx, nx.gz, nxz");
		inputfO.setArgs(1);
		options.addOption(inputfO);
		
		Option ranksO = new Option("r", "name of file containing context ranks (input data should be grouped by context if using ranks)");
		ranksO.setArgs(1);
		options.addOption(ranksO);
		
		Option ranksfO = new Option("rf", "ranks format: nx, nx.gz, nxz");
		ranksfO.setArgs(1);
		options.addOption(ranksfO);
		
		Option docsO = new Option("d", "count docs (input should be grouped by context)");
		docsO.setArgs(0);
		options.addOption(docsO);
		
		Option pldsO = new Option("p", "count plds");
		pldsO.setArgs(0);
		options.addOption(pldsO);
		
		Option cmpO = new Option("cmp", "aggregate container membership properties");
		cmpO.setArgs(0);
		options.addOption(cmpO);
		
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

		int ticks = org.semanticweb.yars.nx.cli.Main.getTicks(cmd);
		
		String[] infiles = cmd.getOptionValues("i");
		
		int format = 0;
		if(cmd.hasOption("if")){
			if(cmd.getOptionValue("if").equals("nx")){
				format = 0;
			} else if(cmd.getOptionValue("if").equals("nx.gz")){
				format = 1;
			} else if(cmd.getOptionValue("if").equals("nxz")){
				format = 2;
			} else{
				System.err.println("***ERROR: illegal value "+cmd.getOptionValue("if")+" for 'if'");
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("parameters:", options );
				return;
			}
		}
		
		Iterator<Node[]>[] iters = new Iterator[infiles.length];
		NodesBlockReader[] nbrs = new NodesBlockReader[infiles.length];
		InputStream[] iss = new InputStream[infiles.length];
		
//		HashSet<Node> contexts = new HashSet<Node>();
		
		if(infiles.length==0){
			System.err.println("***ERROR: no input files specified");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		for(int i=0; i<infiles.length; i++){
			if(format==2){
				if(!cmd.hasOption("i") || cmd.getOptionValue("i").equals("-")){
					System.err.println("***ERROR: need filename for format nxz -- cannot read stdin");
					HelpFormatter formatter = new HelpFormatter();
					formatter.printHelp("parameters:", options );
					return;
				}
				nbrs[i] = new NodesBlockReaderNIO(infiles[i]);
				iters[i] = new ScanIterator(nbrs[i]);		
			} else{
				if(!cmd.hasOption("i") || cmd.getOptionValue("i").equals("-")){
					iss[i] = System.in;
				} else{
					iss[i] = new FileInputStream(infiles[i]);
				}
				
				if(format == 1){
					iss[i] = new GZIPInputStream(iss[i]);
				}
				
				iters[i] = new NxParser(iss[i]);
			}
		}
		
		String ranks = cmd.getOptionValue("r");
		HashMap<Node,Double> docranks = null;
		
		double minrank = Double.MAX_VALUE;
		
		if(ranks!=null){
			Iterator<Node[]> rankIter = null;
			NodesBlockReader rankNbr = null;
			InputStream rankIs = null;
			docranks = new HashMap<Node,Double>();
			
			if(cmd.hasOption("rf")){
				if(cmd.getOptionValue("rf").equals("nx") || cmd.getOptionValue("rf").equals("nx.gz")){
					if(ranks.equals("-")){
						rankIs = System.in;
					} else{
						rankIs = new FileInputStream(ranks);
					}
					
					if(cmd.getOptionValue("rf").equals("nx.gz")){
						rankIs = new GZIPInputStream(rankIs);
					}
					
					rankIter = new NxParser(rankIs);
				} else if(cmd.getOptionValue("rf").equals("nxz")){
					if(ranks.equals("-")){
						System.err.println("***ERROR: need filename for format nxz -- cannot read stdin");
						HelpFormatter formatter = new HelpFormatter();
						formatter.printHelp("parameters:", options );
						return;
					}
					rankNbr = new NodesBlockReaderNIO(ranks);
					rankIter = new ScanIterator(rankNbr);
				} else{
					System.err.println("***ERROR: illegal value "+cmd.getOptionValue("rf")+" for 'rf'");
					HelpFormatter formatter = new HelpFormatter();
					formatter.printHelp("parameters:", options );
					return;
				}
			}
			
			
			_log.info("Loading ranks...");
			while(rankIter.hasNext()){
				Node[] next = rankIter.next();
				Double rank = Double.parseDouble(next[1].toString());
				docranks.put(next[0], rank);
				if(rank<minrank)
					minrank = rank;
			}
			_log.info("...loaded "+docranks.size()+" ranks.");
		}
		
		boolean docs = cmd.hasOption("d");
		boolean plds = cmd.hasOption("p");
		boolean cmp = cmd.hasOption("cmp");
		
		HashMap<Node,TermStats> predstats = new HashMap<Node,TermStats>();
		HashMap<Node,TermStats> classstats = new HashMap<Node,TermStats>();
		HashMap<Node,TermStats> dtstats = new HashMap<Node,TermStats>();
		
		HashMap<String,String> pldFW = null;
		if(plds) pldFW  = new HashMap<String,String>();
		
		long done = 0;
		boolean last = false;
		Node oldCon = null;
		
		Count<Node> preds = new Count<Node>();
		Count<Node> classes = new Count<Node>();
		Count<Node> dts = new Count<Node>();

		_log.info("Scanning data...");
		for(int i=0; i<iters.length; i++){
			Iterator<Node[]> iter = iters[i];
			last = !iter.hasNext();
			while(!last){
				Node[] next = null;
				if(iter.hasNext()){
					next = iter.next();
				} else if(i == iters.length-1){
					last = true; 
				} else{
					break;
				}

				done++;
				if(ticks>0 && done%ticks==0){
					_log.info("...read "+done);
				}
				
				if(last || (oldCon!=null && !oldCon.equals(next[3]))){
					Double rank = null;
					String pld = null;
					
//					if(!contexts.add(oldCon)){
//						_log.info("Repeated context "+oldCon);
//					}
					
					if(docranks!=null && !docranks.isEmpty()){
						rank = docranks.get(oldCon);
					
						if(rank==null){
							_log.info("Missing rank for document "+oldCon);
							rank = minrank;
						}
					}
					
					if(plds){
						pld = getPLD(oldCon);
						String pldf = pldFW.get(pld);
						if(pldf==null){
							pldFW.put(pld, pld);
						} else{
							pld = pldf;
						}
					}
					
					update(preds, predstats, pld, docs, rank);
					update(classes, classstats, pld, docs, rank);
					update(dts, dtstats, pld, docs, rank);
					
					preds = new Count<Node>();
					classes = new Count<Node>();
					dts = new Count<Node>();
				}
				
				if(!last){
					oldCon = next[3];
					
					preds.add(next[1]);
					if(next[1].equals(RDF.TYPE)){
						if(next[2] instanceof Resource)
							classes.add(next[2]);
					}
					else if(cmp && next[1].toN3().startsWith(RDF_CMP_PREFIX)){
						preds.add(RDF_CMP_ALL);
					}
					else if(next[2] instanceof Literal){
						Literal l = (Literal)next[2];
						Resource r = l.getDatatype();
						if(r!=null)
							dts.add(r);
						else
							dts.add(new Resource("no-dt"));
					}
				}
			}
		}
		
		_log.info("...scanned data... read "+done);
		
		for(InputStream is:iss){
			if(is!=null)
				is.close();
		}
		
		for(NodesBlockReader nbr:nbrs){
			if(nbr!=null)
				nbr.close();
		}
		
		_log.info("...dumping stats...");
		
		CallbackNxOutputStream cn = new CallbackNxOutputStream(System.out);
		
		System.out.println("==============================");
		System.out.println("Classes : "+classstats.size());
		System.out.println("==============================");
		
		printStats(classstats, cn);
		
		System.out.println("==============================");
		if(cmp)
			System.out.println("Predicates : "+(predstats.size()-1));
		else System.out.println("Predicates : "+predstats.size());
		System.out.println("==============================");
		
		printStats(predstats, cn);
		
		System.out.println("==============================");
		System.out.println("Datatypes : "+dtstats.size());
		System.out.println("==============================");
		
		printStats(dtstats, cn);
		
		System.out.flush();
		
		_log.info("...stats dumped...");
		
		_log.info("serialising stats object...");
		
		OutputStream os = new FileOutputStream(STATS_OBJ);
		os = new GZIPOutputStream(os);
		ObjectOutputStream oos = new ObjectOutputStream(os);
		
		oos.writeObject(predstats);
		oos.writeObject(classstats);
		oos.writeObject(dtstats);
		
		oos.close();
		
		_log.info("Finished! Enjoy the stats.");
	}
	
	public static String getPLD(Node n){
		if(n instanceof Resource){
			try {
				URI u = new URI(n.toString());
				String pld = TLDM.getPLD(u);
				if(pld!=null){
					return pld;
				}
				return NO_PLD;
			} catch (Exception e) {
				return NO_PLD;
			}
		} 
		return NO_PLD;
	}
	
	public static String getNamespace(Node n){
		try{
			String ns = Namespace.getNamespace(n);
			if(ns!=null)
				return ns;
			return NO_NS;
		} catch(Exception e){
			return NO_NS;
		}
	}
	
	public static Count<Integer> getDistribution(Count<?> c){
		Count<Integer> dist = new Count<Integer>();
		for(Map.Entry<?, Integer> el:c.entrySet()){
			dist.add(el.getValue());
		}
		return dist;
	}
	
	public static Count<String> getUniqueTermDistribution(Count<Node> classes, Count<Node> props){
		Count<String> uniqueNs = new Count<String>();
		for(Map.Entry<Node, Integer> el:classes.entrySet()){
			uniqueNs.add(getNamespace(el.getKey()));
		}
		for(Map.Entry<Node, Integer> el:props.entrySet()){
			uniqueNs.add(getNamespace(el.getKey()));
		}
		return uniqueNs;
	}
	
	public static void update(Count<Node> nodes, HashMap<Node,TermStats> stats, String pld, boolean doc, Double rank){
		for(Map.Entry<Node, Integer> nodeCount: nodes.entrySet()){
			TermStats ts = stats.get(nodeCount.getKey());
			if(ts==null){
				ts = new TermStats();
				stats.put(nodeCount.getKey(), ts);
			}
			
			ts.count+=nodeCount.getValue();
			
			if(doc)
				ts.docs++;
			if(pld!=null)
				ts.pld.add(pld);
			if(rank!=null){
				ts.rankSum+=rank;
				if(rank>ts.rankMax){
					ts.rankMax = rank;
				}
			}
		}
	}
	
	public static void printStats(HashMap<Node,TermStats> stats, Callback cb){
		for(Map.Entry<Node, TermStats> ns:stats.entrySet()){
			cb.processStatement(ns.getValue().toNodes(ns.getKey()));
		}
	}
	
	public static class TermStats implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		int count = 0;
		Count<String> pld = new Count<String>();
		int docs = 0;
		double rankSum = 0;
		double rankMax = 0;
		
		public TermStats(){
			;
		}
		
		public void addTermStats(TermStats ts){
			count += ts.count;
			pld.addAll(ts.pld);
			docs += ts.docs;
			rankSum += ts.rankSum;
			if(ts.rankMax>rankMax) rankMax = ts.rankMax;
		}
		
		public Node[] toNodes(){
			return toNodes(null);
		}
		
		public Node[] toNodes(Node first){
			ArrayList<Node> nodes = new ArrayList<Node>();
			if(first!=null){
				nodes.add(first);
			}
			
			nodes.add(new Literal(Integer.toString(count)));
			if(docs!=0)
				nodes.add(new Literal(Integer.toString(docs)));
			if(!pld.isEmpty())
				nodes.add(new Literal(Integer.toString(pld.size())));
			
			if(rankSum!=0)
				nodes.add(new Literal(Double.toString(rankSum)));
			if(rankMax!=0)
				nodes.add(new Literal(Double.toString(rankMax)));
			
			Node[] na = new Node[nodes.size()];
			nodes.toArray(na);
			
			return na;
		}
	}
}
