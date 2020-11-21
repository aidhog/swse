package org.semanticweb.swse.cli;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

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
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.namespace.VOID;
import org.semanticweb.yars.nx.namespace.XSD;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.stats.Count;

import com.ontologycentral.ldspider.tld.TldManager;

public class GetSomeNiceCSPOStats {
	static transient Logger _log = Logger.getLogger(GetSomeNiceCSPOStats.class.getName());
	
	public static String VOID_BASE_IRI = "http://ex.org/dataset#";
	
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
		
		Option outputO = new Option("o", "output file (if VoID is desired)");
		outputO.setArgs(1);
		options.addOption(outputO);
		
		Option baseO = new Option("b", "base IRI (default "+VOID_BASE_IRI+")");
		baseO.setArgs(1);
		options.addOption(baseO);
		
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
		
		String out = cmd.getOptionValue("o");
		String base = cmd.getOptionValue("b");
		if(base==null)
			base = VOID_BASE_IRI;
		
		BufferedWriter bwVoid = null;
		
		if(out!=null)
			bwVoid = new BufferedWriter(new FileWriter(out));
		
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

		long docs = 0;
		
		Count<String> docsPerPld = new Count<String>();
		HashSet<Node> predicates = new HashSet<Node>();
		HashSet<Node> classes = new HashSet<Node>();
		HashSet<String> linksData = new HashSet<String>();
		HashSet<String> linksAll = new HashSet<String>();
		
		Count<Node> docsPerPred = new Count<Node>();
		Count<Node> docsPerClass = new Count<Node>();
		Count<String> pldLinksData = new Count<String>();
		Count<String> pldLinksAll = new Count<String>();
		
		long done = 0;
		Node[] old = null;
		String pld = NO_PLD;
		for(Iterator<Node[]> iter: iters){
			while(iter.hasNext()){
				Node[] next = iter.next();

				done++;
				if(ticks>0 && done%ticks==0){
					_log.info("Read "+done);
				}
				
				if(old!=null && !old[3].equals(next[3])){
					docsPerPld.add(pld);
					docs++;
					
					predicates = new HashSet<Node>();
					classes = new HashSet<Node>();
					linksData = new HashSet<String>();
					linksAll = new HashSet<String>();
				} 
				
				old = next;
				
				pld = GetSomeNiceSPOCStats.getPLD(next[3]);
				
				if(!pld.equals(NO_PLD)) {
					for(int i=0; i<3; i++) {
						String pldN = GetSomeNiceSPOCStats.getPLD(next[i]);
						if(!pldN.equals(NO_PLD)) {
							if(i!=1 && (i!=2 || !next[1].equals(RDF.TYPE))) {
								if(linksData.add(pldN)) {
									pldLinksData.add(pld+"\t"+pldN);
								}
							}
							if(linksAll.add(pldN)) {
								pldLinksAll.add(pld+"\t"+pldN);
							}
						}
					}
				}
				
				if(predicates.add(next[1])) {
					docsPerPred.add(next[1]);
				}
				
				if(next[1].equals(RDF.TYPE) || next[1].equals(GetSomeNiceSPOCStats.WIKIDATA_TYPE)){
					if(classes.add(next[2])) {
						docsPerClass.add(next[2]);
					}
				}
			}
		}
		
		docsPerPld.add(pld);
		docs++;
		
		System.out.println("==============================");
		System.out.println("Statements : "+done);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Documents: "+docs);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Documents per PLD: "+docsPerPld.size());
		System.out.println("==============================");
		docsPerPld.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Documents per predicate: "+docsPerPred.size());
		System.out.println("==============================");
		docsPerPred.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Documents per class: "+docsPerClass.size());
		System.out.println("==============================");
		docsPerClass.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("All PLD links (unique per document): "+pldLinksAll.size());
		System.out.println("==============================");
		pldLinksAll.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("All PLD links (outdegree): ");
		System.out.println("==============================");
		GetSomeNiceSPOCStats.getPldCount(pldLinksAll).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("All PLD links (indegree): ");
		System.out.println("==============================");
		GetSomeNiceSPOCStats.getPldCount(pldLinksAll,1).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Data PLD links (unique per document): "+pldLinksData.size());
		System.out.println("==============================");
		pldLinksData.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Data PLD links (outdegree): ");
		System.out.println("==============================");
		GetSomeNiceSPOCStats.getPldCount(pldLinksData).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Data PLD links (indegree): ");
		System.out.println("==============================");
		GetSomeNiceSPOCStats.getPldCount(pldLinksData,1).printOrderedStats(System.out);
		System.out.println("==============================");
		
		if(out!=null) {
			Resource subject = new Resource(VOID_BASE_IRI+"this");
			
			bwVoid.write(Nodes.toN3(new Node[] {subject , VOID.DOCUMENTS, new Literal(Long.toString(docs),XSD.INTEGER) })+"\n");
			
			bwVoid.close();
		}
		
		for(InputStream is:iss){
			if(is!=null)
				is.close();
		}
		
		for(NodesBlockReader nbr:nbrs){
			if(nbr!=null)
				nbr.close();
		}
	}
}
