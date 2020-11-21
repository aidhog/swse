package org.semanticweb.swse.econs.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
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
import org.semanticweb.swse.econs.sim.utils.RemoteScatter;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class CheckSimilarities {
	private final static Logger _log = Logger.getLogger(CheckSimilarities.class.getSimpleName());

	public static final int TOP_K = 10;
	
	public static void main(String args[]) throws Exception{
		NxParser.DEFAULT_PARSE_DTS = false;
		Options options = new Options();
		
		Option inO = new Option("i", "similarities to extract");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option dO = new Option("d", "directory of raw similarities");
		dO.setArgs(1);
		dO.setRequired(true);
		options.addOption(dO);
		
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
		
		InputStream is = new FileInputStream(cmd.getOptionValue("i"));
		
		NxParser nxp = new NxParser(is);
		
		HashSet<Nodes> sims = new HashSet<Nodes>();
		
		while(nxp.hasNext()){
			Node[] next = nxp.next();
			sims.add(new Nodes(next[0], next[1]));
		}
		
		is.close();
		
		String dir = cmd.getOptionValue("d");
		
		Vector<String> files = new Vector<String>();
		
		File d = new File(dir);
		for(File f:d.listFiles()){
			if(f.getName().endsWith(RemoteScatter.GATHER)){
				files.add(f.getAbsolutePath());
				_log.info("Adding "+f.getAbsoluteFile()+" to check...");
			}
		}
		
		for(String f:files){
			is = new GZIPInputStream(new FileInputStream(f));
			
			nxp = new NxParser(is);
			
			while(nxp.hasNext()){
				Node[] next = nxp.next();
				Nodes check = new Nodes(next[0], next[1]);
				if(sims.contains(check)){
					_log.info(Nodes.toN3(next));
				}
			}
			is.close();
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
}
