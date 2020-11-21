package org.semanticweb.swse.econs.ercons.utils;


import org.semanticweb.saorr.fragments.Fragment;
import org.semanticweb.saorr.fragments.owl2rl.OWL2RL_T_SPLIT;
import org.semanticweb.saorr.rules.GraphPatternRule;

/**
 * Rules for finding IFPs and FPs
 * 
 * @author Aidan Hogan
 * @date 2010-03-25
 */
public class CONS_RULES extends Fragment{
	public static final GraphPatternRule PRP_EQP1 = OWL2RL_T_SPLIT.PRP_EQP1;
	public static final GraphPatternRule PRP_EQP2 = OWL2RL_T_SPLIT.PRP_EQP2;
	
	public static final GraphPatternRule PRP_INV1 = OWL2RL_T_SPLIT.PRP_INV1;
	public static final GraphPatternRule PRP_INV2 = OWL2RL_T_SPLIT.PRP_INV2;
	
	public static final GraphPatternRule PRP_SYMP = OWL2RL_T_SPLIT.PRP_SYMP;
	
	public static final GraphPatternRule PRP_SPO1 = OWL2RL_T_SPLIT.PRP_SPO1;
	
//	public static final GraphPatternRule CONS_1 = new GraphPatternRule(
//			"cons-ifp",
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.INVERSEFUNCTIONALPROPERTY
//					)
//			},
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.INVERSEFUNCTIONALPROPERTY
//					)
//			}
//	
//	);
//	
//	public static final GraphPatternRule CONS_2 = new GraphPatternRule(
//			"cons-fp",
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.FUNCTIONALPROPERTY
//					)
//			},
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.FUNCTIONALPROPERTY
//					)
//			}
//	
//	);
//	
//	public static final GraphPatternRule CONS_1 = new GraphPatternRule(
//			"cons-sb-ifp",
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDFS.SUBPROPERTYOF,
//							new Variable("p2")
//					),
//					new Statement(new Variable("p2"),
//							RDF.TYPE,
//							OWL.INVERSEFUNCTIONALPROPERTY
//					)
//			},
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.INVERSEFUNCTIONALPROPERTY
//					)
//			}
//	);
//	
//	public static final GraphPatternRule CONS_2 = new GraphPatternRule(
//			"cons-sb-fp",
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDFS.SUBPROPERTYOF,
//							new Variable("p2")
//					),
//					new Statement(new Variable("p2"),
//							RDF.TYPE,
//							OWL.FUNCTIONALPROPERTY
//					)
//			},
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.FUNCTIONALPROPERTY
//					)
//			}
//	);
//	
//	public static final GraphPatternRule CONS_3 = new GraphPatternRule(
//			"cons-sp-tran",
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDFS.SUBPROPERTYOF,
//							new Variable("p2")
//					),
//					new Statement(new Variable("p2"),
//							RDFS.SUBPROPERTYOF,
//							new Variable("p3")
//					)
//			},
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDFS.SUBPROPERTYOF,
//							new Variable("p3")
//					)
//			}
//	);
//	
//	public static final GraphPatternRule CONS_4 = new GraphPatternRule(
//			"cons4",
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							OWL.EQUIVALENTPROPERTY,
//							new Variable("p2")
//					),
//			},
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDFS.SUBPROPERTYOF,
//							new Variable("p2")
//					)
//			}
//	);
//	
//	public static final GraphPatternRule CONS_5 = new GraphPatternRule(
//			"cons5",
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							OWL.EQUIVALENTPROPERTY,
//							new Variable("p2")
//					),
//			},
//			null,
//			new Statement[]{
//
//					new Statement(new Variable("p2"),
//							RDFS.SUBPROPERTYOF,
//							new Variable("p1")
//					)
//			}
//	);
//	
//	public static final GraphPatternRule CONS_6 = new GraphPatternRule(
//			"cons-inv-fp-ifp",
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							OWL.INVERSEOF,
//							new Variable("p2")
//					),
//					new Statement(new Variable("p2"),
//							RDF.TYPE,
//							OWL.FUNCTIONALPROPERTY
//					)
//			},
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.INVERSEFUNCTIONALPROPERTY
//					)
//			}
//	);
//	
//	public static final GraphPatternRule CONS_7 = new GraphPatternRule(
//			"cons-inv-ifp-fp",
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							OWL.INVERSEOF,
//							new Variable("p2")
//					),
//					new Statement(new Variable("p2"),
//							RDF.TYPE,
//							OWL.INVERSEFUNCTIONALPROPERTY
//					)
//			},
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.FUNCTIONALPROPERTY
//					)
//			}
//	);
//	
//	public static final GraphPatternRule CONS_10 = new GraphPatternRule(
//			"cons-sym-fp-ifp",
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.SYMMETRICPROPERTY
//					),
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.FUNCTIONALPROPERTY
//					)
//			},
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.INVERSEFUNCTIONALPROPERTY
//					)
//			}
//	);
//	
//	public static final GraphPatternRule CONS_11 = new GraphPatternRule(
//			"cons-sym-ifp-fp",
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.SYMMETRICPROPERTY
//					),
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.INVERSEFUNCTIONALPROPERTY
//					)
//			},
//			null,
//			new Statement[]{
//					new Statement(new Variable("p1"),
//							RDF.TYPE,
//							OWL.FUNCTIONALPROPERTY
//					)
//			}
//	);
}
