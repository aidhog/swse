package org.semanticweb.swse.ann.rank.utils;

import java.util.TreeSet;

import junit.framework.TestCase;

import org.junit.Test;
import org.semanticweb.swse.ann.rank.utils.AggregateTripleRanksIterator;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;

public class AggregateTripleRanksTest extends TestCase{
	private static final Node[][] DATA = {
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new BNode("x1"), new Literal("0.1")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new BNode("x2"), new Literal("0.1")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new BNode("x3"), new Literal("0.2")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new BNode("x4"), new Literal("0.3")},
		new Node[]{ new BNode("c"), new BNode("d"), new BNode("e"), new BNode("x1"), new Literal("0.4")},
		new Node[]{ new BNode("c"), new BNode("d"), new BNode("f"), new BNode("x1"), new Literal("0.5")},
		new Node[]{ new BNode("c"), new BNode("d"), new BNode("g"), new BNode("x1"), new Literal("0.2")},
		new Node[]{ new BNode("c"), new BNode("d"), new BNode("g"), new BNode("x2"), new Literal("0.2")},
		new Node[]{ new BNode("h"), new BNode("k"), new BNode("m"), new BNode("x1"), new Literal("0.3")},
		new Node[]{ new BNode("h"), new BNode("k"), new BNode("m"), new BNode("x2"), new Literal("0.2")}
	};
	
	private static final Node[][] AGG_DATA = {
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new BNode("x1"), new Literal("0.7")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new BNode("x2"), new Literal("0.7")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new BNode("x3"), new Literal("0.7")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new BNode("x4"), new Literal("0.7")},
		new Node[]{ new BNode("c"), new BNode("d"), new BNode("e"), new BNode("x1"), new Literal("0.4")},
		new Node[]{ new BNode("c"), new BNode("d"), new BNode("f"), new BNode("x1"),  new Literal("0.5")},
		new Node[]{ new BNode("c"), new BNode("d"), new BNode("g"), new BNode("x1"), new Literal("0.4")},
		new Node[]{ new BNode("c"), new BNode("d"), new BNode("g"), new BNode("x2"), new Literal("0.4")},
		new Node[]{ new BNode("h"), new BNode("k"), new BNode("m"), new BNode("x1"), new Literal("0.5")},
		new Node[]{ new BNode("h"), new BNode("k"), new BNode("m"), new BNode("x2"), new Literal("0.5")}
	};
	
	@Test
	public void testAggregateTriples() throws Exception{
		NodeComparator nc = new NodeComparator(true, true);
		TreeSet<Node[]> sorted = new TreeSet<Node[]>(nc);
		for(Node[] d:DATA)
			sorted.add(d);
		
		AggregateTripleRanksIterator atr = new AggregateTripleRanksIterator(sorted.iterator());
		
		int i = 0;
		while(atr.hasNext()){
			assertTrue(i<AGG_DATA.length);
			
			Node[] next = atr.next();
			Node[] expected = AGG_DATA[i];
			
			System.err.println(Nodes.toN3(next));
			
			assertTrue(NodeComparator.NC.equals(dropLast(next), dropLast(AGG_DATA[i])));
			
			assertEquals(Double.parseDouble(expected[expected.length-1].toString()), Double.parseDouble(next[next.length-1].toString()));
			i++;
		}
		
		assertEquals(i, AGG_DATA.length);
	}
	
	public static final Node[] dropLast(Node[] na){
		Node[] noLast = new Node[na.length-1];
		System.arraycopy(na, 0, noLast, 0, noLast.length);
		return noLast;
	}
}
