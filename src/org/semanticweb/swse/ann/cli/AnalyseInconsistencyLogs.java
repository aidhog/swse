package org.semanticweb.swse.ann.cli;

import java.io.File;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.saorr.engine.ih.InconsistencyLogParser;
import org.semanticweb.saorr.engine.ih.InconsistencyLogParser.InconsistencyInfo;
import org.semanticweb.saorr.rules.TemplateRule;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.stats.Count;
import org.semanticweb.yars.util.LRUSetCache;


/**
 * Main method to setup a reasoning service which can be run via RMI.
 * 
 * @author aidhog
 */
public class AnalyseInconsistencyLogs {
	
	static Logger _log = Logger.getLogger(AnalyseInconsistencyLogs.class.getName());
	
	public static final int TICKS = 100000;
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, AlreadyBoundException{
		Option inputO = new Option("i", "input");
		inputO.setArgs(1);
		inputO.setRequired(true);
		
		Option gzO = new Option("gz", "gz");
		gzO.setArgs(0);
		
		Option helpO = new Option("h", "print help");
				
		Options options = new Options();
		options.addOption(inputO);
		options.addOption(gzO);
		options.addOption(helpO);

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

		String input = null;
		if (cmd.hasOption("i")) {
			input = cmd.getOptionValue("i");
		}
		
		boolean gz = false;
		if (cmd.hasOption("gz")) {
			gz = true;
		}
		
		analyseInconsistencyLogs(input, gz);
	}
	
	public static void analyseInconsistencyLogs(String file, boolean gz) throws IOException{
		File f = new File(file);
		InconsistencyLogParser ilp = new InconsistencyLogParser(f, gz);
		
		HashMap<String, InconsistencyStats> statsPerRawRule = new HashMap<String, InconsistencyStats>();
		HashMap<String, InconsistencyStats> statsPerRule = new HashMap<String, InconsistencyStats>();
		
		InconsistencyStats all = newInconsistencyStats();
		
		int c = 0;
		
		while(ilp.hasNext()){
			InconsistencyInfo ii = ilp.next();
			c++;
			
			all.addInconsistency(ii);
			
			if(c%TICKS==0){
				_log.info("Analysed "+c+" inconsistencies");
			}
			String rule = ii.getRuleId();
			String rawRule = TemplateRule.getOriginalRuleId(rule);
			
			InconsistencyStats isr = statsPerRawRule.get(rawRule);
			if(isr==null){
				isr = newInconsistencyStats();
				statsPerRawRule.put(rawRule, isr);
			}
			isr.addInconsistency(ii);
			
			if(!rule.equals(rawRule)){
				isr = statsPerRule.get(rule);
				if(isr==null){
					isr = newInconsistencyStats();
					statsPerRule.put(rule, isr);
				}
				isr.addInconsistency(ii);
			}
			
		}
		
		_log.info("================================");
		_log.info("ALL");
		_log.info("================================");
		all.logStats();
		_log.info("================================");
		for(Map.Entry<String, InconsistencyStats> ris:statsPerRawRule.entrySet()){
			_log.info("================================");
			_log.info(ris.getKey());
			_log.info("================================");
			ris.getValue().logStats();
			_log.info("================================");
		}
		
//		for(Map.Entry<String, InconsistencyStats> ris:statsPerRule.entrySet()){
//			_log.info("================================");
//			_log.info(ris.getKey());
//			_log.info("================================");
//			ris.getValue().logStats();
//			_log.info("================================");
//		}
		
	}
	
	private static InconsistencyStats newInconsistencyStats(){
		return new InconsistencyStats(new LRUSetCache<Nodes>(1000));
//		return new InconsistencyStats();
	}
	
	private static Nodes flatten(Collection<Nodes> set){
		int length = 0;
		for(Nodes n:set){
			length += n.getNodes().length;
		}
		
		Node[] flat = new Node[length];
		
		
		int pos = 0;
		for(Nodes n:set){
			Node[] na = n.getNodes();
			System.arraycopy(na, 0, flat, pos, na.length);
			pos += na.length;
		}
		
		return new Nodes(flat);
	}
	
	public static class InconsistencyStats{
		private int inconsistencyCount = 0;
		
		private int _violationCount = 0;
		private int _uniqueViolationCount = 0;
		
		private int _uVAequalsCA = 0;
		
		private Set<Nodes> _maxViolation;
		private double _maxViolationDegree = Double.MIN_VALUE;
		private String _maxViolationRule;
		
		private Set<Nodes> _maxViolated;
		private double _maxViolatedDegree = Double.MIN_VALUE;
		private String _maxViolatedRule;
		
		private double _sumViolationDegree = 0;
		private double _sumUniqueViolationDegree = 0;
		private double _sumConstraintAnnotation = 0;
		private double _sumUniqueConstraintAnnotation = 0;
		
		private int _equals = 0;
		
		private Set<Nodes> _uniqueViolations;
		
		private Count<String> _tGroundRules = new Count<String>();
		
		public InconsistencyStats(){
			this(new HashSet<Nodes>());
		}
		
		public InconsistencyStats(Set<Nodes> uniqueViolations){
			_uniqueViolations = uniqueViolations;
		}
		
		public void logStats(){
			_log.info("Inconsistencies "+inconsistencyCount);
			_log.info("Violations "+_violationCount);
			_log.info("Unique violations "+_uniqueViolationCount);
			
			_log.info("Max violation Degree "+_maxViolationDegree);
			_log.info("Max violation Rule "+_maxViolationRule);
			_log.info("Max violation Data");
			for(Nodes n:_maxViolation){
				_log.info(n.toString());
			}
			_log.info("Max violated Degree "+_maxViolatedDegree);
			_log.info("Max violated Rule "+_maxViolatedRule);
			_log.info("Max violated Data");
			for(Nodes n:_maxViolated){
				_log.info(n.toString());
			}
			
			_log.info("Equals "+_equals);
			
			_log.info("Sum violation degree "+_sumViolationDegree);
			_log.info("Average violation degree "+_sumViolationDegree/(double)_violationCount);
			
			_log.info("Sum unique violation degree "+_sumUniqueViolationDegree);
			_log.info("Average unique violation degree "+_sumUniqueViolationDegree/(double)_uniqueViolations.size());
			
			_log.info("Sum constraint annotation "+_sumConstraintAnnotation);
			_log.info("Average constraint annotation "+_sumConstraintAnnotation/(double)inconsistencyCount);
			
			_log.info("T-ground instance rules "+_tGroundRules.size());
			
			_log.info("Sum unique constraint annotation "+_sumUniqueConstraintAnnotation);
			_log.info("Average unique constraint annotation "+_sumUniqueConstraintAnnotation/(double)_tGroundRules.size());
			
			_log.info("Unique violations whose degree is given by constraint "+_uVAequalsCA);
			
			_log.info("T-ground instance count:\n");
			_tGroundRules.printOrderedStats(_log, Level.INFO);
		}
		
		public void addInconsistency(InconsistencyInfo ii){
			inconsistencyCount++;
			_violationCount+=ii.getData().size();
			double cann = toRank(ii.getRuleAnnotation());
			
			_sumConstraintAnnotation += cann;
			if(_tGroundRules.add(ii.getRuleId())==1){
				_sumUniqueConstraintAnnotation += cann;
			}

			for(int i=0; i<ii.getData().size(); i++){
				Set<Nodes> data = ii.getData().get(i);
				Nodes ann = ii.getDataAnnotations().get(i);
				double vann = toRank(ann);
				
				_sumViolationDegree+=vann;
				
				Nodes n = flatten(data);
				if(_uniqueViolations.add(n)){
					_uniqueViolationCount++;
					_sumUniqueViolationDegree+=vann;
					
					if(vann==cann){
						_uVAequalsCA++;
					}
					
					if(vann>_maxViolationDegree){
						_maxViolationDegree = vann;
						_maxViolationRule = ii.getRuleId();
						_maxViolation = data;
					}
					
					double[] dataAnns = toRanks(data);
					
					double maxData = max(dataAnns);
					
					if(maxData>_maxViolatedDegree){
						_maxViolatedDegree = maxData;
						_maxViolatedRule = ii.getRuleId();
						_maxViolated = data;
					}
					
					if(allEquals(dataAnns)){
						_equals++;
					}
				}
			}
		}
		
		private static double toRank(Nodes ann){
			Node[] annNa = ann.getNodes();
			return Double.parseDouble(annNa[annNa.length-1].toString());
		}
		
		private static double[] toRanks(Collection<Nodes> anns){
			double[] ranks = new double[anns.size()];
			
			int pos = 0;
			for(Nodes ann:anns){
				ranks[pos] = toRank(ann);
				pos++;
			}
			return ranks;
		}
		
		private static double max(double[] ranks){
			double max = Double.MIN_VALUE;
			
			for(double r:ranks){
				if(r>max)
					max = r;
			}
			
			return max;
		}
		
		private static boolean allEquals(double[] ranks){
			if(ranks.length<2)
				return true;
			
			double first = ranks[0];
			for(double rank:ranks){
				if(rank!=first)
					return false;
			}
			return true;
		}
	}
}