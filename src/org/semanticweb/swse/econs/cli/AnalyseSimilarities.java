package org.semanticweb.swse.econs.cli;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.cons.utils.SameAsIndex;
import org.semanticweb.swse.econs.sim.RMIEconsSimServer.PredStats;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class AnalyseSimilarities {
	private final static Logger _log = Logger.getLogger(AnalyseSimilarities.class.getSimpleName());

	public static final int TOP_K = 50;
	
	public static void main(String args[]) throws Exception{
		NxParser.DEFAULT_PARSE_DTS = false;
		Options options = new Options();
		
		Option inO = new Option("i", "similarities to extract");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option rankO = new Option("r", "ranks file");
		rankO.setArgs(1);
		rankO.setRequired(true);
		options.addOption(rankO);
		
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
		
		InputStream is = new FileInputStream(cmd.getOptionValue("r"));
		
		NxParser nxp = new NxParser(is);
		
		HashSet<Node> ranked = new HashSet<Node>();
		
		while(nxp.hasNext()){
			Node[] next = nxp.next();
			ranked.add(next[0]);
		}
		
		_log.info("Rank list "+ranked);
		
		is.close();
		
		is = new GZIPInputStream(new FileInputStream(cmd.getOptionValue("i")));
		
		nxp = new NxParser(is);
			
		int all = 0, interpld = 0, intrapld = 0, nopld = 0;
		
		
		double sumintra = 0, suminter = 0, sumnopld = 0;
		
		
		int subs = 1;
		Node old = null;
		while(nxp.hasNext()){
			Node[] next = nxp.next();
			all++;
			
			if(old!=null && !old.equals(next[0])){
				subs++;
			}
			
			old = next[0];
			
			if(ranked.contains(next[0]) || ranked.contains(next[1])){
				_log.info("Ranked "+Nodes.toN3(next));
			}
			
			String plda = SameAsIndex.getPLD(next[0]);
			String pldb = SameAsIndex.getPLD(next[1]);
			
			if(plda==null || pldb==null){
				nopld++;
				sumnopld += Double.parseDouble(next[2].toString());
			} else if(plda.equals(pldb)){
				interpld++;
				sumintra += Double.parseDouble(next[2].toString());
			} else{
				intrapld++;
				suminter += Double.parseDouble(next[2].toString());
			}
		}
		
		_log.info("Tuples "+all);
		_log.info("Intra-pld "+intrapld+" sum "+sumintra);
		_log.info("Inter-pld "+interpld+" sum "+suminter);
		_log.info("No-pld "+nopld+" sum "+sumnopld);
		_log.info("Subs "+subs);
		_log.info("Avg. "+((double)all/(double)subs));
		
		is.close();
	}
	
	public static void logStats(HashMap<Node,PredStats> predStat, int topk){
		TreeSet<Map.Entry<Node,PredStats>> entryStats = new TreeSet<Map.Entry<Node,PredStats>>(new PredStatComparator());
		entryStats.addAll(predStat.entrySet());
		
		int i = 0;
		_log.info("===Top "+topk+"===");
		for(Map.Entry<Node,PredStats> e:entryStats){
			_log.info(e.getKey().toN3()+" "+e.getValue());
			i++;
			if(i==topk) break;
		}
		
		Iterator<Map.Entry<Node,PredStats>> reverse = entryStats.descendingIterator();
		
		i = 0;
		_log.info("===Bottom "+topk+"===");
		while(reverse.hasNext()&&i<topk){
			Map.Entry<Node,PredStats> e = reverse.next();
			_log.info(e.getKey().toN3()+" "+e.getValue());
			i++;
		}
	}
	
	public static class PredStatComparator implements Comparator<Map.Entry<Node,PredStats>>{

		public int compare(Entry<Node, PredStats> arg0,
				Entry<Node, PredStats> arg1) {
			int comp = Double.compare(arg0.getValue().getAverage(), arg1.getValue().getAverage());
			if(comp!=0){
				return comp;
			}
			
			return arg0.getKey().compareTo(arg1.getKey());
		}
		
	}
	
	public static class PivotSet<E> extends TreeSet<E>{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		Node _p;
		
		public PivotSet(){
			super();
		}
		
		public void setPivot(Node n){
			_p = n;
		}
		
		public String toString(){
			StringBuilder sb = new StringBuilder();
			sb.append("Pivot "+_p+" ");
			sb.append(super.toString());
			return sb.toString();
		}
	}
	
	public static class SetSizeComparator<E> implements Comparator<Set<E>>{

		public int compare(Set<E> arg0, Set<E> arg1) {
			int comp = Double.compare(arg0.size(), arg1.size());
			if(comp==0){
				return -1;
			}
			return comp;
		}
		
	}
	
	public static class SetRandomComparator<E> implements Comparator<Set<E>>{

		public int compare(Set<E> arg0, Set<E> arg1) {
			int comp = Double.compare(arg0.iterator().next().hashCode(), arg1.iterator().next().hashCode());
			if(comp==0){
				return -1;
			}
			return comp;
		}
		
	}
}
