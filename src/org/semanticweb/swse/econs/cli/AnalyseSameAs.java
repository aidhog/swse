package org.semanticweb.swse.econs.cli;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.swse.cons.utils.SameAsIndex.SameAsList;
import org.semanticweb.swse.cons.utils.SameAsIndex.SameAsStatistics;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class AnalyseSameAs {
	private final static Logger _log = Logger.getLogger(AnalyseSameAs.class.getSimpleName());

	public static final int TOP_K = 5;
	public static final int RANDOM_K = 100;
	
	public static void main(String args[]) throws Exception{
		NxParser.DEFAULT_PARSE_DTS = false;
		Options options = new Options();
		
		int tk = TOP_K;
		int rk = RANDOM_K;

		Option inO = new Option("i", "gz nx sameas file s1<s2");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option tO = new Option("t", "record top t class sizes");
		tO.setArgs(1);
		options.addOption(tO);
		
		Option rO = new Option("r", "record r random classes");
		rO.setArgs(1);
		options.addOption(rO);
		
		Option rankO = new Option("ranks", "ranks file");
		rankO.setArgs(1);
		options.addOption(rankO);
		
		Option sO = new Option("s", "skip s1 s2 if s1<s2");
		options.addOption(sO);
		
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
		
		boolean skip = cmd.hasOption("s");
		
		if(cmd.hasOption("t")){
			tk = Integer.parseInt(cmd.getOptionValue("t"));
		}
		if(cmd.hasOption("r")){
			rk = Integer.parseInt(cmd.getOptionValue("r"));
		}
		
		
		String ranks = cmd.getOptionValue("ranks");
		HashSet<Node> ranked = new HashSet<Node>();
		if(ranks!=null){
			int r = 0;
			InputStream isr = new GZIPInputStream(new FileInputStream(ranks));
			NxParser riter = new NxParser(isr);
			while(riter.hasNext() && r<tk){
				Node[] next = riter.next();
				ranked.add(next[0]);
				r++;
				_log.info("Loading ranked "+r+" "+Nodes.toN3(next));
			}
		}
		
		InputStream is = new GZIPInputStream(new FileInputStream(cmd.getOptionValue("i")));
		Iterator<Node[]> nxp = new NxParser(is);

		SameAsStatistics sas = new SameAsStatistics(tk, rk);
		int stmt = 0;
		Node old = null;
		SameAsList eclass = new SameAsList();
		
		while(nxp.hasNext()){
			Node[] next = nxp.next();
			
			if(skip && next[0].compareTo(next[2])>0){
				continue;
			}
			stmt++;
			
			if(old!=null && !old.equals(next[0])){
				eclass.setPivot(old);
				eclass.add(old);
				sas.addSameasList(eclass);
				
				HashSet<Node> rnked = new HashSet<Node>(); 
				if(ranked.contains(old))
					rnked.add(old);
				for(Node n:eclass){
					if(ranked.contains(n)){
						rnked.add(n);
					}
				}
				
				if(!rnked.isEmpty()){
					_log.info("Ranked EQC Ranked "+rnked+" EQCS Size "+eclass.size()+" EQCS "+eclass);
				}
				
				eclass = new SameAsList();
			}
			old = next[0];
			eclass.add(next[2]);
		}
		
		eclass.setPivot(old);
		eclass.add(old);
		sas.addSameasList(eclass);
		eclass = new SameAsList();
		
		sas.logStats(_log, Level.INFO);
		is.close();
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
