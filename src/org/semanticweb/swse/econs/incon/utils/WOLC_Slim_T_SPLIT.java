package org.semanticweb.swse.econs.incon.utils;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.fragments.owl2rl.OWL2RL_T_SPLIT;
import org.semanticweb.saorr.fragments.owlhogan.WOL_T_SPLIT;
import org.semanticweb.saorr.rules.GraphPatternRule;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.swse.econs.incon.SlaveEconsInconArgs.FuzzyMatchOperator;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars2.query.algebra.filter.bool.Equals;
import org.semanticweb.yars2.query.algebra.filter.bool.Not;
import org.semanticweb.yars2.query.algebra.filter.noop.Noop;

/**
 * Ontological Web Language (WOL) rules
 * 
 * Class with OWL2RL rules statically defined
 * (currently incomplete)
 * 
 * @author Aidan Hogan
 * @date 2009-10-01
 */
public class WOLC_Slim_T_SPLIT extends WOL_T_SPLIT {
//	public static final GraphPatternRule EQ_DIFF1 = OWL2RL_T_SPLIT.EQ_DIFF1;
	
//	public static final GraphPatternRule EQ_DIFF2 = OWL2RL_T_SPLIT.EQ_DIFF2;
	
//	public static final GraphPatternRule EQ_DIFF3 = OWL2RL_T_SPLIT.EQ_DIFF3;
	
	
	public static final GraphPatternRule PRP_IRP = OWL2RL_T_SPLIT.PRP_IRP;
	
	public static final GraphPatternRule PRP_ASYP = OWL2RL_T_SPLIT.PRP_ASYP;
	
	public static final GraphPatternRule PRP_PDW = OWL2RL_T_SPLIT.PRP_PDW;
	
	public static final GraphPatternRule PRP_ADP = OWL2RL_T_SPLIT.PRP_ADP;
	
//	public static final GraphPatternRule PRP_NPA1 = OWL2RL_T_SPLIT.PRP_NPA1;
	
//	public static final GraphPatternRule PRP_NPA2 = OWL2RL_T_SPLIT.PRP_NPA2;
	
	
//	public static final GraphPatternRule CLS_NOTHING2 = OWL2RL_T_SPLIT.CLS_NOTHING2;
	
	public static final GraphPatternRule CLS_COM = OWL2RL_T_SPLIT.CLS_COM;
	
	public static final GraphPatternRule CLS_MAXC1 = OWL2RL_T_SPLIT.CLS_MAXC1;
	
//	public static final GraphPatternRule CLS_MAXQC1 = OWL2RL_T_SPLIT.CLS_MAXQC1;
	
	public static final GraphPatternRule CLS_MAXQC2 = OWL2RL_T_SPLIT.CLS_MAXQC2;
	
	
	public static final GraphPatternRule CAX_DW = OWL2RL_T_SPLIT.CAX_DW;
	
	public static final GraphPatternRule CAX_ADC = OWL2RL_T_SPLIT.CAX_ADC;
	
	public static final GraphPatternRule REF_DIFF1 = new GraphPatternRule(
			"ref-diff1",
			new Statement[]{
					new Statement(new Variable("x"),
							OWL.DIFFERENTFROM,
							new Variable("y")
					)
					
			},
			new Equals(new Noop(new Variable("x")), new Noop(new Variable("y"))),
			Rule.FALSE_CONSEQUENT
	);
	
	public static final GraphPatternRule PRP_FP_C = new GraphPatternRule(
			"prp-ifp-c",
			new Statement[]{
					new Statement(new Variable("x"),
							new Variable("p"),
							new Variable("y1")
					),
					new Statement(new Variable("x"),
							new Variable("p"),
							new Variable("y2")
					)
			},
			new Statement[]{
					new Statement(new Variable("p"),
							RDF.TYPE,
							OWL.FUNCTIONALPROPERTY)
			},
			new Not(new FuzzyMatchOperator(new Noop(new Variable("y1")), new Noop(new Variable("y2")))),
			Rule.FALSE_CONSEQUENT
	);
//	public static final Rule DT_NOT_TYPE = OWL2RL_T_SPLIT.DT_NOT_TYPE;
}
