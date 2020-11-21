package org.semanticweb.swse.cli;

import java.io.File;
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
import org.semanticweb.saorr.engine.Reasoner;
import org.semanticweb.saorr.engine.ReasonerEnvironment;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.input.NxaGzInput;
import org.semanticweb.saorr.fragments.Fragment;
import org.semanticweb.saorr.fragments.owlhogan.WOL_T_SPLIT;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.saorr.rules.SortedRuleSet;
import org.semanticweb.swse.saor.master.MasterReasoner;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.NodeComparator.NodeComparatorArgs;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator.MergeSortArgs;
import org.semanticweb.yars.stats.Count;

import com.ontologycentral.ldspider.hooks.content.CallbackDummy;

public class CheckTemplateRuleStats {
	static transient Logger _log = Logger.getLogger(CheckTemplateRuleStats.class.getName());
	
	public final static String DIR = ".";
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Options	options = new Options();
		
		org.semanticweb.yars.nx.cli.Main.addInputOption(options, "i", "gzipped");
		
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

		String infile = cmd.getOptionValue("i");
		
		_log.info("Reading TBox from "+infile);
		_log.info("Pass 1: not merged...");
		NxaGzInput tboxin = new NxaGzInput(new File(infile));
		
		Rules rules = new Rules(new Rules(Fragment.getRules(WOL_T_SPLIT.class)).getTboxAboxRules());
//		MasterReasoner.buildTbox(nxgz, new Rules(r.getTboxAboxRules()), new CallbackDummy());
		
		
		SortedRuleSet tboxAndAxiom = Rules.toSet(rules.getTboxRules());
		tboxAndAxiom.addAll(Rules.toSet(rules.getAxiomaticRules()));
		
		ReasonerSettings rs = new ReasonerSettings();
		rs.setAuthorativeReasoning(true);
		rs.setFragment(tboxAndAxiom);
		rs.setMergeRules(false);
		rs.setPrintContexts(true);
		rs.setSaturateRules(false);
		rs.setSkipABox(true);
		rs.setSkipAxiomatic(false);
		rs.setSkipTBox(false);
		rs.setTBoxRecursion(false);
		rs.setTemplateRules(true);
		rs.setUseAboxRuleIndex(true);
		
		ReasonerEnvironment re = new ReasonerEnvironment(null, tboxin, new CallbackDummy());
		
		Reasoner r = new Reasoner(rs, re);
		r.reason();
		
		_log.info("Pass 2: merged...");
		tboxin.reset();
		
		tboxAndAxiom = Rules.toSet(rules.getTboxRules());
		tboxAndAxiom.addAll(Rules.toSet(rules.getAxiomaticRules()));
		
		rs = new ReasonerSettings();
		rs.setAuthorativeReasoning(true);
		rs.setFragment(tboxAndAxiom);
		rs.setMergeRules(true);
		rs.setPrintContexts(true);
		rs.setSaturateRules(false);
		rs.setSkipABox(true);
		rs.setSkipAxiomatic(false);
		rs.setSkipTBox(false);
		rs.setTBoxRecursion(false);
		rs.setTemplateRules(true);
		rs.setUseAboxRuleIndex(true);
		
		re = new ReasonerEnvironment(null, tboxin, new CallbackDummy());
		
		r = new Reasoner(rs, re);
		r.reason();
		
		tboxin.close();
		_log.info("done.");
		
	}
}
