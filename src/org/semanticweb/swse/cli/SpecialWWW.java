package org.semanticweb.swse.cli;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.stats.Count;
import org.semanticweb.yars.util.CallbackNxBufferedWriter;

/**
 * Deadline-surfing special
 * @author aidhog
 *
 */
public class SpecialWWW {
	static transient Logger _log = Logger.getLogger(SpecialWWW.class.getName());
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws org.semanticweb.yars.nx.parser.ParseException 
	 */
	public static void main(String[] args) throws IOException, org.semanticweb.yars.nx.parser.ParseException {
		
		Options options = org.semanticweb.yars.nx.cli.Main.getStandardOptions();

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
		
		InputStream is = org.semanticweb.yars.nx.cli.Main.getMainInputStream(cmd);
		OutputStream os = org.semanticweb.yars.nx.cli.Main.getMainOutputStream(cmd);
		long ticks = org.semanticweb.yars.nx.cli.Main.getTicks(cmd);
		
		Iterator<Node[]> it = new NxParser(is);
		
		OutputStreamWriter osw = new OutputStreamWriter(os);
		BufferedWriter bw = new BufferedWriter(osw);
		CallbackNxBufferedWriter cb = new CallbackNxBufferedWriter(bw);
		
		long c = 0;
		Node[] old = null;
		
		String rdfp = "<"+RDF.NS;
		
		Count<Node> clas = new Count<Node>();
		Count<Node> pred = new Count<Node>();
		Count<Node> predl = new Count<Node>();
		Count<Node> dt = new Count<Node>();
		
		int dupe = 0, write = 0, unique = 0;
		
		_log.info("Reading input...");
		System.out.println("...stats will be written to stdout...");
		
		while(it.hasNext()){
			Node[] next = it.next();
			
			String p = next[1].toN3();
			boolean rtype = false;
			
			if(p.startsWith(rdfp)){
				if(next[1].equals(RDF.TYPE)){
					rtype = true;
					String cl = next[2].toN3();
					if(cl.startsWith(rdfp)){
						cb.processStatement(next);
						write++;
					}
				} else{
					cb.processStatement(next);
					write++;
				}
			}
			
			if(old==null || !old[2].equals(next[2]) || !old[1].equals(next[1]) || !old[0].equals(next[0])){
				unique ++;
				pred.add(next[1]);
				if(rtype)
					clas.add(next[2]);
				if(next[2] instanceof Literal){
					predl.add(next[1]);
					Literal l = (Literal)next[2];
					if(l.getDatatype()!=null)
						dt.add(l.getDatatype());
				}
			} else{
				dupe++;
			}
			
			old = next;
			
			c++;
			
			if(ticks>0 && c%ticks==0){
				_log.info("...Read "+c+". Written "+write+". Dupe trips "+dupe+". Unique trips "+unique+".");
			}
		}
		
		_log.info("Finished: Read "+c+". Written "+write+". Dupe trips "+dupe+". Unique trips "+unique+".");
		
		System.out.println("=====Classes "+clas.size()+"=====");
		clas.printOrderedStats(System.out);
		
		System.out.println("=====Predicates "+pred.size()+"=====");
		pred.printOrderedStats(System.out);
		
		System.out.println("=====Predicates w/ lit "+pred.size()+"=====");
		predl.printOrderedStats(System.out);
		
		System.out.println("=====Datatypes "+dt.size()+"=====");
		dt.printOrderedStats(System.out);
		
		System.out.flush();
		
		is.close();
		bw.close();
	}

}
