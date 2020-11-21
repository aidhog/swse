package org.semanticweb.swse.cli;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.Namespace;
import org.semanticweb.yars.stats.Count;

public class PatchStats {
	static transient Logger _log = Logger.getLogger(PatchStats.class.getName());
	
	final static String NO_NS = "no_ns";
	final static String NO_PLD = "no_pld";
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws org.semanticweb.yars.nx.parser.ParseException 
	 */
	public static void main(String[] args) throws org.semanticweb.yars.nx.parser.ParseException, IOException {
		Count<String> predNs = new Count<String>();
		Count<String> classNs = new Count<String>();
		Count<String> allNs = new Count<String>();
		
		BufferedReader br1 = new BufferedReader(new FileReader("testdata/preds.txt"));
		BufferedReader br2 = new BufferedReader(new FileReader("testdata/classes.txt"));
		
		String line = null;
		
		FileOutputStream fos = new FileOutputStream("testdata/ns.large.txt");
		PrintStream ps = new PrintStream(fos);
		
		while((line = br1.readLine())!=null){
			line = line.trim();
			if(!line.isEmpty()){
				StringTokenizer tok = new StringTokenizer(line, " \t");

				if(tok.countTokens()!=2){
					throw new RuntimeException("Cannot parse pred line "+line);
				}
				String ns = getNamespace(new Resource(tok.nextToken()));
				predNs.add(ns);
				allNs.add(ns);
			}
		}
		
		while((line = br2.readLine())!=null){
			line = line.trim();
			if(!line.isEmpty()){
				StringTokenizer tok = new StringTokenizer(line, " \t");

				if(tok.countTokens()!=2){
					throw new RuntimeException("Cannot parse class line "+line);
				}
				String ns = getNamespace(new Resource(tok.nextToken()));
				classNs.add(ns);
				allNs.add(ns);
			}
		}
		
		ps.println("Class ns:");
		classNs.printOrderedStats(ps);
		
		ps.println("Pred ns:");
		predNs.printOrderedStats(ps);
		
		ps.println("All ns:");
		allNs.printOrderedStats(ps);
		
		ps.close();
		fos.close();
		br1.close();
		br2.close();
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
}
