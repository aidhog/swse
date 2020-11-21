package org.semanticweb.swse.cli;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.NodeComparator.NodeComparatorArgs;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator.MergeSortArgs;
import org.semanticweb.yars.stats.Count;

public class CheckReasonedDupes {
	static transient Logger _log = Logger.getLogger(CheckReasonedDupes.class.getName());
	
	public final static String DIR = ".";
	/**
	 * @param args
	 * @throws IOException 
	 * @throws org.semanticweb.yars.nx.parser.ParseException 
	 */
	public static void main(String[] args) throws org.semanticweb.yars.nx.parser.ParseException, IOException {
		Options	options = new Options();
		
		org.semanticweb.yars.nx.cli.Main.addTicksOption(options);
		org.semanticweb.yars.nx.cli.Main.addInputsOption(options, "i", "");
		
		
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
		
		InputStream[] iss = org.semanticweb.yars.nx.cli.Main.getMainInputStreams(cmd);
		Iterator<Node[]>[] iters = new Iterator[iss.length];
		for (int i=0; i< iss.length; i++) {
			iters[i] = new NxParser(iss[i]);
		}
		_log.info("Opened "+iters.length+" files for checking duplicates");
		
		NodeComparatorArgs nca = new NodeComparatorArgs();
		nca.setNoEquals(true);
		nca.setNoZero(true);
		
		MergeSortArgs msa = new MergeSortArgs(iters);
		msa.setComparator(new NodeComparator(nca));
		msa.setTicks(ticks);
		
		MergeSortIterator msi = new MergeSortIterator(msa);
		
		Node[] tripleold = null;
		
		NodeComparatorArgs ncav = new NodeComparatorArgs();
		ncav.setVarLength(true);
		NodeComparator ncv = new NodeComparator(ncav);
		
		Count<Node> dupes = new Count<Node>();
		Count<Node> triples = new Count<Node>();
		Count<Node> unique1 = new Count<Node>();
		Count<Node> unique2 = new Count<Node>();
		Count<Node> total = new Count<Node>();
		Count<String> cooccur = new Count<String>();
		
		ArrayList<Node> cons = new ArrayList<Node>();
		
		Resource asserted = new Resource("asserted");
		
		boolean a  = false, r = false;
		
		long reasonQ = 0;
		long reasonT = 0;
		long uniqueReasonT = 0;
		
		long assertedQ = 0;
		long assertedT = 0;
		long uniqueAssertedT = 0;
		
		long quads = 0;
		
		long uniqueT = 0;
		
		boolean done = !msi.hasNext();
		Node[] quad = null;
		while(!done){
			
			if(msi.hasNext()){
				quad = msi.next();
				quads++;
			} else{
				done = true;
			}
			
			if(tripleold==null){
				tripleold = new Node[3];
				System.arraycopy(quad, 0, tripleold, 0, 3);
			} else if(done || !ncv.equals(tripleold, quad)){
				uniqueT++;
				if(cons.size()==1){
					unique1.add(cons.get(0));
					unique2.add(cons.get(0));
					triples.add(cons.get(0));
				} else{
					TreeSet<Node> consu = new TreeSet<Node>();
					TreeSet<Node> selfdupe = new TreeSet<Node>();
					for(Node n:cons){
						dupes.add(n);
						if(!consu.add(n))
							selfdupe.add(n);
					}
					
					if(consu.size()==1){
						unique2.add(consu.first());
					}
					
					for(Node n:consu){
						triples.add(n);
					}
					
					for(Node n:consu){
						for(Node m:consu){
							if(n.compareTo(m)<0){
								cooccur.add(n.toString()+" "+m.toString());
							}
						}
					}
					
					for(Node s:selfdupe){
						cooccur.add(s.toString()+" "+s.toString());
					}
				}
				
				if(!a){
					uniqueReasonT++;
					reasonT++;
				} else{
					assertedT++;
					if(r){
						reasonT++;
					} else{
						uniqueAssertedT++;
					}
				}
				
				cons = new ArrayList<Node>();
				a = false;
				r = false;
				System.arraycopy(quad, 0, tripleold, 0, 3);
			}
			
			if(!done){
				if(quad[3].toString().startsWith(Rule.CONTEXT_PREFIX)){
					reasonQ++;
					Resource con = parseRuleContext((Resource)quad[3]);
					cons.add(con);
					total.add(con);
					r = true;
				} else{
					assertedQ++;
					total.add(asserted);
					if(!a){
						cons.add(asserted);
						a = true;
					}
				}
			}
		}
		
		_log.info("Finished duplicate check. Sorted "+msi.count()+" with "+msi.duplicates()+" duplicates.");
		
		_log.info("Found "+quads+" quads.");
		_log.info("Found "+uniqueT+" triples.");
		
		_log.info("Found "+assertedQ+" asserted quads.");
		_log.info("Found "+assertedT+" asserted triples.");
		_log.info("Found "+uniqueAssertedT+" only asserted triples.");
		
		_log.info("Found "+reasonQ+" reasoned quads.");
		_log.info("Found "+reasonT+" reasoned triples.");
		_log.info("Found "+uniqueReasonT+" only reasoned triples.");
		
		
		for(Node n:total.keySet()){
			_log.info("Context "+n+" q:"+total.get(n)+" t:"+triples.get(n)+" u1:"+unique1.get(n)+" u2:"+unique2.get(n)+" d:"+dupes.get(n));
		}
		
		for(Map.Entry<String,Integer> e:cooccur.entrySet()){
			_log.info("Cooccur "+e.getKey()+" "+e.getValue());
		}
		
		
		for(InputStream is:iss){
			is.close();
		}
	}
	
	private static Resource parseRuleContext(Resource r){
		String ruleId = r.toString().substring(Rule.CONTEXT_PREFIX.length());
		if(ruleId.startsWith("tmp")){
			String[] parsed = ruleId.split("_");
			if(parsed.length!=3)
				throw new RuntimeException("Cannot parse rule "+r);
			return new Resource(Rule.CONTEXT_PREFIX+parsed[1]);
		}
		return r;
	}
}
