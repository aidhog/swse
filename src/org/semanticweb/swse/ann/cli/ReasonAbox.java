package org.semanticweb.swse.ann.cli;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.saorr.ann.AnnotatedStatement;
import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.domains.RankAnnotationFactory;
import org.semanticweb.saorr.ann.engine.AnnotationReasoner;
import org.semanticweb.saorr.ann.engine.AnnotationReasonerEnvironment;
import org.semanticweb.saorr.ann.engine.unique.UniqueAnnotatedStatementFilter;
import org.semanticweb.saorr.ann.engine.unique.UniquingAnnotationHashset;
import org.semanticweb.saorr.ann.index.AnnotatedStatementStore;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.input.FileInput;
import org.semanticweb.saorr.engine.input.NxGzInput;
import org.semanticweb.saorr.engine.input.NxInput;
import org.semanticweb.saorr.engine.input.NxaGzInput;
import org.semanticweb.saorr.fragments.Fragment;
import org.semanticweb.saorr.fragments.owlhogan.WOL_T_SPLIT;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.saorr.rules.SortedRuleSet;
import org.semanticweb.swse.ann.reason.master.MasterAnnReasoner;
import org.semanticweb.swse.ann.reason.utils.ResetableFlyweightNodeIterator;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.namespace.RDF;

import com.ontologycentral.ldspider.hooks.content.CallbackDummy;


/**
 * Main method to setup ranking which can be run via RMI.
 * 
 * @author aidhog
 */
public class ReasonAbox {
	
	static Logger _log = Logger.getLogger(ReasonAbox.class.getName());
	
	public static void main(String args[]) throws Exception{
		long time = System.currentTimeMillis();
		
		Option inputTO = new Option("it", "input gz tbox data");
		inputTO.setArgs(1);
		
		Option inputAO = new Option("ia", "input abox data");
		inputAO.setArgs(1);
		
		Option lrO = new Option("l", "linked rules");
		
		Option helpO = new Option("h", "print help");
		
		Option authO = new Option("a", "authoritative reasoning");
				
		Options options = new Options();
		options.addOption(inputTO);
		options.addOption(inputAO);
		options.addOption(helpO);
		options.addOption(authO);
		options.addOption(lrO);

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

		String inputT = null;
		if (cmd.hasOption("it")) {
			inputT = cmd.getOptionValue("it");
		}
		
		String inputA = null;
		if (cmd.hasOption("ia")) {
			inputA = cmd.getOptionValue("ia");
		}
		
		boolean auth = cmd.hasOption("a");
		boolean lr = cmd.hasOption("l");
		
		reasonABox(inputA, inputT, auth, lr);
		
		long time1 = System.currentTimeMillis();
	    
		_log.info("Tbox reasoned in " + (time1-time) + " ms.");
	}
	
	public static void reasonABox(String inA, String inT, boolean auth, boolean lr) throws Exception{
		
		_log.info("Reasoning over abox "+inA+" and tbox "+inT);
		FileInput tboxIn = null;
		if(auth){
			tboxIn = new NxaGzInput(new File(inT),4);
		} else{
			tboxIn = new NxGzInput(new File(inT));
		}
		ResetableFlyweightNodeIterator fwiter = new ResetableFlyweightNodeIterator(1000, tboxIn);
		
		Rules rs = new Rules(Fragment.getRules(WOL_T_SPLIT.class));
		if(auth)
			rs.setAuthoritative();
		
		CallbackDummy cs = new CallbackDummy();
		
		LinkedRuleIndex<AnnotationRule<RankAnnotation>> tmplRules = null;
		AnnotatedStatementStore<RankAnnotation> tbox = null;
		if(lr){
			tmplRules = MasterAnnReasoner.buildTbox(fwiter, new Rules(rs.getAboxRules()), cs, auth);
			
//			for(LinkedRule<AnnotationRule<RankAnnotation>> tmplRule : tmplRules.getAllLinkedRules()){
//				System.err.println(tmplRule.getRule()+" (links "+tmplRule.linksCount()+")");
//			}
		} else{
			ReasonerSettings res = new ReasonerSettings();
			res.setAuthorativeReasoning(auth);
			res.setFragment(rs.getAboxRules());
			res.setPrintContexts(true);
			res.setSkipABox(true);
			res.setSkipAxiomatic(false);
			res.setSkipTBox(false);
			res.setTBoxRecursion(false);
			
			AnnotationReasonerEnvironment<RankAnnotation> re = new AnnotationReasonerEnvironment<RankAnnotation>(null, tboxIn, RankAnnotationFactory.SINGLETON, cs);
			
			AnnotationReasoner<RankAnnotation> r = new AnnotationReasoner<RankAnnotation>(res, re);
			r.reason();
			
			tbox = re.tBox;
		}
		
		_log.info("... moving onto A-Box");
		FileInput aboxIn = new NxInput(new File(inA));
		
		ReasonerSettings rset = new ReasonerSettings();
		rset.setSkipTBox(true);
		rset.setSkipAxiomatic(true);
		rset.setPrintContexts(true);
		
		AnnotationReasonerEnvironment<RankAnnotation> re = new AnnotationReasonerEnvironment<RankAnnotation>(aboxIn, RankAnnotationFactory.SINGLETON, cs);
		if(lr){
			re.setAboxRuleIndex(tmplRules);
			rset.setUseAboxRuleIndex(true);
		} else{
			re.setTBox(tbox);
		}
		UniqueAnnotatedStatementFilter<RankAnnotation> uasf = new UniquingAnnotationHashset<RankAnnotation>();
		re.setUniqueStatementFilter(uasf);
		
		AnnotationReasoner<RankAnnotation> r = new AnnotationReasoner<RankAnnotation>(rset, re);
		RankAnnotationFactory raf = RankAnnotationFactory.SINGLETON;
		
		SortedRuleSet<AnnotationRule<RankAnnotation>> aboxRules = new SortedRuleSet<AnnotationRule<RankAnnotation>>();
		for(Rule rul:rs.getAboxRules()){
			aboxRules.add(raf.wrapRule(rul));
		}
		
		while(aboxIn.hasNext()){
			Node[] next = aboxIn.next();
			
			Set<Node> dataNodes = getDataNodes(next);
			
			
			AnnotatedStatement<RankAnnotation> as = raf.fromNodes(next);
			HashSet<AnnotatedStatement<RankAnnotation>> set = new HashSet<AnnotatedStatement<RankAnnotation>>();
			r.reasonAbox(as, aboxRules, set);
		
			_log.info("============In================");
			_log.info(as.toString());
			_log.info("============Out================");
			int i = 0;
			for(AnnotatedStatement<RankAnnotation> s:set){
				
				if(intersects(s.toNodeTriple(), dataNodes)){
					_log.info("DATA : "+s.toString());
					i++;
				} else{
					_log.info("SCHEMA : "+s.toString());
				}
			}
			_log.info("Out size "+set.size());
			_log.info("Data out size "+i);
			
			uasf.clear();
		}
		_log.info("... done");
		tboxIn.close();
		aboxIn.close();
		
	}
	
	public static boolean intersects(Node[] a, Set<Node> b){
		for(Node n:a){
			if(b.contains(n))
				return true;
		}
		return false;
	}
	
	public static Set<Node> getDataNodes(Node[] data){
		HashSet<Node> dns = new HashSet<Node>();
		dns.add(data[0]);
		if(!data[1].equals(RDF.TYPE)){
			dns.add(data[2]);
		}
		return dns;
	}
}