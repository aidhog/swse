package org.semanticweb.swse.cli;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.commons.cli.ParseException;
import org.semanticweb.nxindex.ScanIterator;
import org.semanticweb.nxindex.block.NodesBlockReader;
import org.semanticweb.nxindex.block.NodesBlockReaderNIO;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.Namespace;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.namespace.VOID;
import org.semanticweb.yars.nx.namespace.XSD;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.stats.Count;
import org.semanticweb.yars.tld.TldManager;

public class GetSomeNiceSPOCStats {
	static transient Logger _log = Logger.getLogger(GetSomeNiceSPOCStats.class.getName());
	
	public static String VOID_BASE_IRI = "http://ex.org/dataset#";
	
	final static Resource WIKIDATA_TYPE = new Resource("http://www.wikidata.org/prop/direct/P31");
	
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
		
		Option uniqueO = new Option("u", "only consider unique triples (input triples must be grouped)");
		uniqueO.setArgs(0);
		options.addOption(uniqueO);
		
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
		
		boolean unique = cmd.hasOption("u");
		
		long triples = 0;
		
		long entities = 0, sameasOnly = 0, bnodeSubj = 0, uriSubj = 0;
		
		Count<Node> preds = new Count<Node>();
		Count<Node> classes = new Count<Node>();
		Count<Node> classTriples = new Count<Node>();
		
		Count<String> predsPerPld = new Count<String>();
		Count<String> classesPerPld = new Count<String>();
		Count<String> nsPerPld = new Count<String>();
		
		TreeSet<Node> subjClasses = new TreeSet<Node>();
		
		Count<String> predns = new Count<String>();
		Count<String> classns = new Count<String>();
		Count<String> allns = new Count<String>();
		
		Count<String> pldLinksAll = new Count<String>();
		Count<String> pldLinksData = new Count<String>();
		
		Count<String> plds = new Count<String>();
		
		Count<Integer> subjDist = new Count<Integer>();
		
		long litObj = 0, bnodeObj = 0, uriObj = 0;
		
		long done = 0;
		Node[] old = null;
		int sc = 0;
		boolean nsa = false;
		for(Iterator<Node[]> iter: iters){
			while(iter.hasNext()){
				Node[] next = iter.next();

				done++;
				if(ticks>0 && done%ticks==0){
					_log.info("Read "+done);
				}
				
				if(old!=null && !old[0].equals(next[0])){
					subjDist.add(sc);
					entities++;
					
					if(old[0] instanceof BNode){
						bnodeSubj++;
					} else{
						uriSubj++;
					}
					
					if(!nsa){
						sameasOnly++;
					}
					
					for(Node c:subjClasses){
						classTriples.add(c, sc);
					}
					
					nsa = false;
					sc = 0;
					
					subjClasses = new TreeSet<Node>();
				} else if(old!=null && old[0].equals(next[0]) && old[1].equals(next[1]) && old[2].equals(next[2])){
					if(unique)
						continue;
					else
						triples--;
				}
				
				triples ++;
				
				old = next;
					
				sc ++;
				
				String pld = getPLD(next[3]);
				plds.add(pld);
				
				if(!pld.equals(NO_PLD)) {
					for(int i=0; i<3; i++) {
						String pldN = getPLD(next[i]);
						if(!pldN.equals(NO_PLD)) {
							if(i!=1 && (i!=2 || !next[1].equals(RDF.TYPE))) {
								pldLinksData.add(pld+"\t"+pldN);
							}
							pldLinksAll.add(pld+"\t"+pldN);
						}
					}
				}
				
				preds.add(next[1]);
				String pns = getNamespace(next[1]);
				predns.add(pns);
				allns.add(pns);
				nsPerPld.add(pns+"\t"+pld);
				predsPerPld.add(next[1]+"\t"+pld);
				
				if(!next[1].equals(OWL.SAMEAS)){
					nsa = true;
				}
				
				if(next[1].equals(RDF.TYPE) || next[1].equals(WIKIDATA_TYPE)){
					if(subjClasses.add(next[2])){
						classes.add(next[2]);
						String cns = getNamespace(next[2]);
						classns.add(cns);
						allns.add(cns);
						nsPerPld.add(cns+"\t"+pld);
					}
					classesPerPld.add(next[2]+"\t"+pld);
				}
				
				if(next[2] instanceof Literal){
					litObj++;
				} else if(next[2] instanceof Resource){
					uriObj++;
				} else{
					bnodeObj++;
				}
				
			}
		}
		
		subjDist.add(sc);
		entities++;
		
		if(old[0] instanceof BNode){
			bnodeSubj++;
		} else{
			uriSubj++;
		}
		
		if(nsa){
			sameasOnly++;
		}
		
		for(Node c:subjClasses){
			classTriples.add(c, sc);
		}
		
		System.out.println("==============================");
		System.out.println("Statements : "+done);
		System.out.println("Unique Triples : "+triples);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Entity info (subject info -- assumes input data grouped by S): ");
		System.out.println("==============================");
		System.out.println("Entities: "+entities);
		System.out.println("Entities with only sameAs edges: "+sameasOnly+" ");
		System.out.println("BNode entities: "+bnodeSubj);
		System.out.println("URI entities: "+uriSubj);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Entity triple distribution: ");
		System.out.println("==============================");
		subjDist.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Predicates: "+preds.size());
		System.out.println("==============================");
		preds.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Predicates distribution: ");
		System.out.println("==============================");
		getDistribution(preds).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Predicate namespaces: "+predns.size());
		System.out.println("==============================");
		predns.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Predicate namespaces distribution: ");
		System.out.println("==============================");
		getDistribution(predns).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Predicates per PLD : "+predsPerPld.size());
		System.out.println("==============================");
		predsPerPld.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Predicates PLD count: ");
		System.out.println("==============================");
		getPldCount(predsPerPld).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Classes: "+classes.size());
		System.out.println("==============================");
		classes.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Classes distribution: ");
		System.out.println("==============================");
		getDistribution(classes).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Classes per PLD: "+classesPerPld.size());
		System.out.println("==============================");
		classesPerPld.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Class PLD count: ");
		System.out.println("==============================");
		getPldCount(classesPerPld).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Classes namespaces: "+classns.size());
		System.out.println("==============================");
		classns.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Classes namespaces distribution: ");
		System.out.println("==============================");
		getDistribution(classns).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Classes partitions: "+classTriples.size());
		System.out.println("==============================");
		classTriples.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Schema namespaces: "+allns.size());
		System.out.println("==============================");
		allns.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Schema namespaces distribution: ");
		System.out.println("==============================");
		getDistribution(allns).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Schema namespaces per PLD : "+nsPerPld.size());
		System.out.println("==============================");
		nsPerPld.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Schema namespaces PLD count: ");
		System.out.println("==============================");
		getPldCount(nsPerPld).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Schema namespaces unique terms:");
		System.out.println("==============================");
		getUniqueTermDistribution(classes, preds).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Source PLDs: "+plds.size());
		System.out.println("==============================");
		plds.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Source PLD triple count distribution: ");
		System.out.println("==============================");
		getDistribution(plds).printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("PLD links all: "+pldLinksAll.size());
		System.out.println("==============================");
		pldLinksAll.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("PLD links data: "+pldLinksData.size());
		System.out.println("==============================");
		pldLinksData.printOrderedStats(System.out);
		System.out.println("==============================");
		
		System.out.println("==============================");
		System.out.println("Object info: ");
		System.out.println("==============================");
		System.out.println("Literal objects "+litObj);
		System.out.println("BNode objects "+bnodeObj);
		System.out.println("URI objects "+uriObj);
		System.out.println("==============================");
		
		if(out!=null) {
			Resource subject = new Resource(VOID_BASE_IRI+"this");
			
			bwVoid.write(Nodes.toN3(new Node[] {subject , RDF.TYPE, VOID.DATASET })+"\n");
			
			for(String ns: allns.keySet()) {
				if(!ns.equals(NO_NS)) {
					bwVoid.write(Nodes.toN3(new Node[] {subject , VOID.VOCABULARY, new Resource(ns) })+"\n");
				}
			}
			
			bwVoid.write(Nodes.toN3(new Node[] {subject , VOID.TRIPLES, new Literal(Long.toString(triples), XSD.INTEGER) })+"\n");
			bwVoid.write(Nodes.toN3(new Node[] {subject , VOID.CLASSES, new Literal(Long.toString(classes.size()), XSD.INTEGER) })+"\n");
			bwVoid.write(Nodes.toN3(new Node[] {subject , VOID.PROPERTIES, new Literal(Long.toString(preds.size()), XSD.INTEGER) })+"\n");
			bwVoid.write(Nodes.toN3(new Node[] {subject , VOID.DISTINCTSUBJECTS, new Literal(Long.toString(entities), XSD.INTEGER) })+"\n");
			
			int bnode = 0;
			for(Entry<Node,Integer> cp: classTriples.entrySet()) {
				BNode classPartition = new BNode("voidClass"+bnode);
				bnode++;
				bwVoid.write(Nodes.toN3(new Node[] {subject , VOID.CLASSPARTITION, classPartition })+"\n");
				bwVoid.write(Nodes.toN3(new Node[] {classPartition , VOID.CLASS, cp.getKey() })+"\n");
				bwVoid.write(Nodes.toN3(new Node[] {classPartition , VOID.TRIPLES, new Literal(Long.toString(cp.getValue()), XSD.INTEGER) })+"\n");
			}
			
			bnode = 0;
			for(Entry<Node,Integer> pp: preds.entrySet()) {
				BNode propPartition = new BNode("voidProp"+bnode);
				bnode++;
				bwVoid.write(Nodes.toN3(new Node[] {subject , VOID.PROPERTYPARTITION, propPartition })+"\n");
				bwVoid.write(Nodes.toN3(new Node[] {propPartition , VOID.PROPERTY, pp.getKey() })+"\n");
				bwVoid.write(Nodes.toN3(new Node[] {propPartition , VOID.TRIPLES, new Literal(Long.toString(pp.getValue()), XSD.INTEGER) })+"\n");
			}
			
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
	
	public static String getPLD(Node n){
		if(n instanceof Resource){
			try {
				String iri = n.toString();
//				if(iri.toLowerCase().startsWith("https")) {
//					iri = "http"+iri.substring(5);
//				}
				URI u = new URI(iri);
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
	
	public static Count<String> getPldCount(Count<String> xPerPld, int k){
		Count<String> xPldCount = new Count<String>();
		for(Map.Entry<String, Integer> el:xPerPld.entrySet()){
			xPldCount.add(el.getKey().split("\t")[k]);
		}
		return xPldCount;
	}
	
	public static Count<String> getPldCount(Count<String> xPerPld){
		return getPldCount(xPerPld,0);
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
}
