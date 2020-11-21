package org.semanticweb.swse.econs.ercons.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.junit.Test;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.rank.utils.AggregateTripleRanksIterator;
import org.semanticweb.swse.econs.ercons.master.utils.CardinalityReasoner;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;

public class CardinalityReasonerTest extends TestCase{
	private static final HashMap<Node,HashSet<Node>> CARDS = new HashMap<Node,HashSet<Node>>();
	
	static{
		HashSet<Node> ca1 = new HashSet<Node>();
		ca1.add(new Resource("p1"));
		ca1.add(new Resource("pa"));
		
		CARDS.put(new Resource("ca1"), ca1);
		CARDS.put(new Resource("ca2"), ca1);
	}
	
	private static final Node[][] DATA = {
		new Node[]{ new BNode("a"), new Resource("p1"), new BNode("s1"), new BNode("x1")},
		new Node[]{ new BNode("a"), new Resource("p1"), new BNode("s2"), new BNode("x2")},
		new Node[]{ new BNode("a"), new Resource("pa"), new BNode("t1"), new BNode("x3")},
		new Node[]{ new BNode("a"), new Resource("pa"), new BNode("t2"), new BNode("x4")},
		new Node[]{ new BNode("a"), new Resource("pa"), new BNode("t3"), new BNode("x4")},
		new Node[]{ new BNode("a"), RDF.TYPE, new Resource("ca1"), new BNode("x4")},
		new Node[]{ new BNode("s1"), new Resource("p1"), new BNode("u1"), new BNode("x1")},
		new Node[]{ new BNode("s2"), new Resource("p1"), new BNode("u2"), new BNode("x2")},
		new Node[]{ new BNode("s1"), new Resource("pa"), new BNode("v1"), new BNode("x3")},
		new Node[]{ new BNode("s2"), new Resource("pa"), new BNode("v2"), new BNode("x4")},
		new Node[]{ new BNode("s1"), new Resource("pa"), new BNode("v3"), new BNode("x4")},
		new Node[]{ new BNode("s1"), RDF.TYPE, new Resource("ca2"), new BNode("x4")},
	};
	
	private static final Node[][] SAME_AS = {
		new Node[]{ new BNode("s1"), OWL.SAMEAS, new BNode("s2")},
		new Node[]{ new BNode("t1"), OWL.SAMEAS, new BNode("t2")},
		new Node[]{ new BNode("t1"), OWL.SAMEAS, new BNode("t3")},
		new Node[]{ new BNode("u1"), OWL.SAMEAS, new BNode("u2")},
		new Node[]{ new BNode("v1"), OWL.SAMEAS, new BNode("v2")},
		new Node[]{ new BNode("v1"), OWL.SAMEAS, new BNode("v3")},
	};
	
	private static final String DIR = "testdata/testcardr/";
	
	@Test
	public void testAggregateTriples() throws Exception{
		RMIUtils.mkdirs(DIR);
//		NodeComparator nc = new NodeComparator(true, true);
//		TreeSet<Node[]> sorted = new TreeSet<Node[]>(nc);
		CardinalityReasoner cr = new CardinalityReasoner(DIR, CARDS, false);
		
		
		
		for(Node[] d:DATA)
			cr.addStatement(d);
		
		Iterator<Node[]> sa = cr.performReasoning(null);
		
		TreeSet<Node[]> allsa = new TreeSet<Node[]>(NodeComparator.NC);
		while(sa.hasNext()){
			Node[] next = sa.next();
			allsa.add(next);
			Node[] inv = new Node[]{next[2], OWL.SAMEAS, next[0], next[3]};
			allsa.add(inv);
			System.err.println(Nodes.toN3(next));
		}
		
		sa = cr.performReasoning(allsa.iterator());
		while(sa.hasNext()){
			Node[] next = sa.next();
			allsa.add(next);
			
			System.err.println(Nodes.toN3(next));
		}
		
	}
	
	public static final Node[] dropLast(Node[] na){
		Node[] noLast = new Node[na.length-1];
		System.arraycopy(na, 0, noLast, 0, noLast.length);
		return noLast;
	}
}
