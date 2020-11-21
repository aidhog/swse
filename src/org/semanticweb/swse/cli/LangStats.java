package org.semanticweb.swse.cli;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.stats.Count;

public class LangStats {
	static transient Logger _log = Logger.getLogger(LangStats.class.getName());
	
	static final String NO_TLD = "noTLD";
	static final String LIT = "literal";
	static final String BNODE = "bnode";
	
	static final String NO_LANG = "noLang";
	static final String NOT_LIT = "noLit";
	
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
		_log.info("Opened "+iters.length+" files for checking language");
		
		
		Count<String> langs = new Count<String>();
		
		Count<String> subjs = new Count<String>();
		Count<String> objs = new Count<String>();
		
		Count<String> cons = new Count<String>();
		
		Count<String> data = new Count<String>();
		
		Count<String> preds = new Count<String>();
		Count<String> types = new Count<String>();
		
		Count<String> langCons = new Count<String>();
		
		long done = 0;
		for(Iterator<Node[]> iter: iters){
			while(iter.hasNext()){
				Node[] next = iter.next();
				done++;
				if(ticks>0 && done%ticks==0){
					_log.info("Read "+done);
				}
				
				String lang;
				if(next[2] instanceof Literal){
					Literal l = (Literal)next[2];
					if(l.getLanguageTag()!=null){
						lang = l.getLanguageTag();
					} else{
						lang = NO_LANG;
					}
				} else{
					lang = NOT_LIT;
				}
				
				langs.add(lang);
				
				String tlds = getTLD(next[0]);
				String tldp = getTLD(next[1]);
				String tldo = getTLD(next[2]);
				String tldc = getTLD(next[3]);
				
				subjs.add(tlds);
				preds.add(tldp);
				objs.add(tldo);
				cons.add(tldc);
				
				data.add(tlds);
				
				if(next[1].equals(RDF.TYPE)){
					types.add(tldo);
				} else{
					data.add(tldo);
				}
				
				langCons.add(lang+"\t"+tldc);
			}
		}
		
		_log.info("Read "+done+" statements...");
		
		_log.info("=======================");
		_log.info("LANGUAGES");
		_log.info("=======================");
		langs.printOrderedStats(_log, Level.INFO);
		_log.info("=======================");
		
		_log.info("=======================");
		_log.info("TLDS SUBJECTS");
		_log.info("=======================");
		subjs.printOrderedStats(_log, Level.INFO);
		_log.info("=======================");
		
		_log.info("=======================");
		_log.info("TLDS OBJECTS");
		_log.info("=======================");
		objs.printOrderedStats(_log, Level.INFO);
		_log.info("=======================");
		
		_log.info("=======================");
		_log.info("TLDS CONTEXTS");
		_log.info("=======================");
		cons.printOrderedStats(_log, Level.INFO);
		_log.info("=======================");
		
		_log.info("=======================");
		_log.info("TLDS DATA URIS (subj, obj of non-rdf:type)");
		_log.info("=======================");
		data.printOrderedStats(_log, Level.INFO);
		_log.info("=======================");
		
		_log.info("=======================");
		_log.info("TLDS PREDS");
		_log.info("=======================");
		preds.printOrderedStats(_log, Level.INFO);
		_log.info("=======================");
		
		_log.info("=======================");
		_log.info("TLDS TYPES");
		_log.info("=======================");
		types.printOrderedStats(_log, Level.INFO);
		_log.info("=======================");
		
		_log.info("=======================");
		_log.info("LANGS/TLDS CONTEXTS");
		_log.info("=======================");
		langCons.printOrderedStats(_log, Level.INFO);
		_log.info("=======================");
		
		for(InputStream is:iss)
			is.close();
	}
	
	public static String getTLD(Node n){
		if(n instanceof Resource){
			return getTLD((Resource)n);
		} else if(n instanceof Literal){
			return LIT;
		}
		return BNODE;
	}
	
	public static String getTLD(Resource r){
		try{
			String host = r.getHost();
			if(host==null || host.isEmpty())
				return NO_TLD;
			String[] split = host.split("\\.");
			return split[split.length-1];
		} catch(Exception e){
			return NO_TLD;
		}
	}
}
