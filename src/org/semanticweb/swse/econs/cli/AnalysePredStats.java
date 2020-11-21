package org.semanticweb.swse.econs.cli;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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
import org.semanticweb.swse.econs.sim.RMIEconsSimServer.PredStats;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class AnalysePredStats {
	private final static Logger _log = Logger.getLogger(AnalysePredStats.class.getSimpleName());

	public static final int TOP_K = 10;
	
	public static void main(String args[]) throws Exception{
		NxParser.DEFAULT_PARSE_DTS = false;
		Options options = new Options();
		
		int tk = TOP_K;
		
		Option inO = new Option("i", "pred stats file");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option tO = new Option("t", "record top/bottom tk pred cards");
		tO.setArgs(1);
		options.addOption(tO);
		
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
		
		if(cmd.hasOption("t")){
			tk = Integer.parseInt(cmd.getOptionValue("t"));
		}

		ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(cmd.getOptionValue("i"))));
		ArrayList<HashMap<Node,PredStats>> pStats = ArrayList.class.cast(ois.readObject());
		
		
		_log.info("=====Predicate-Subject=====");
		logStats(pStats.get(0), tk);
		
		_log.info("=====Predicate-Object=====");
		logStats(pStats.get(1), tk);
		
		ois.close();
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
