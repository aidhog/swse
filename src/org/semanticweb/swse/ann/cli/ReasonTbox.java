package org.semanticweb.swse.ann.cli;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.engine.input.NxaGzInput;
import org.semanticweb.saorr.fragments.Fragment;
import org.semanticweb.saorr.fragments.owlhogan.WOL_T_SPLIT;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.saorr.rules.LinkedRuleIndex.LinkedRule;
import org.semanticweb.swse.ann.reason.master.MasterAnnReasoner;
import org.semanticweb.swse.ann.reason.utils.ResetableFlyweightNodeIterator;
import org.semanticweb.yars.util.CallbackNxOutputStream;


/**
 * Main method to setup ranking which can be run via RMI.
 * 
 * @author aidhog
 */
public class ReasonTbox {
	
	public static void main(String args[]) throws Exception{
		long time = System.currentTimeMillis();
		
		Option inputO = new Option("i", "input gz tbox data");
		inputO.setArgs(1);
		
		Option outputO = new Option("o", "output gz reasoned tbox data");
		outputO.setArgs(1);

		Option helpO = new Option("h", "print help");
				
		Options options = new Options();
		options.addOption(inputO);
		options.addOption(outputO);
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
		
		String output = null;
		if (cmd.hasOption("o")) {
			output = cmd.getOptionValue("o");
		}
		
		reasonTBox(input, output);
		
		long time1 = System.currentTimeMillis();
	    
	    System.err.println("Tbox reasoned in " + (time1-time) + " ms.");
	}
	
	public static void reasonTBox(String in, String out) throws Exception{
		System.err.println("Reasoning over "+in+" to "+out);
		NxaGzInput tboxIn = new NxaGzInput(new File(in),4);
		ResetableFlyweightNodeIterator fwiter = new ResetableFlyweightNodeIterator(1000, tboxIn);
		
		Rules rs = new Rules(Fragment.getRules(WOL_T_SPLIT.class));
		rs.setAuthoritative();
		
		OutputStream os = new GZIPOutputStream(new FileOutputStream(out)); 
		CallbackNxOutputStream cnqos = new CallbackNxOutputStream(os);
		
		LinkedRuleIndex<AnnotationRule<RankAnnotation>> tmplRules = MasterAnnReasoner.buildTbox(fwiter, rs, cnqos, true);
		
		os.close();
		tboxIn.close();
		
		for(LinkedRule<AnnotationRule<RankAnnotation>> tmplRule : tmplRules.getAllLinkedRules()){
			System.err.println(tmplRule.getRule()+" (links "+tmplRule.linksCount()+")");
		}
	}
}