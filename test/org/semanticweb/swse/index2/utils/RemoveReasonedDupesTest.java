package org.semanticweb.swse.index2.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.junit.Test;
import org.semanticweb.saorr.rules.DefaultRule;
import org.semanticweb.swse.index2.utils.RemoveReasonedDupesIterator;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;

public class RemoveReasonedDupesTest extends TestCase{
	private static final Node[][] DATA = {
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new Resource("http://aaa.com")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new Resource("http://bbb.com")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new Resource(DefaultRule.CONTEXT_PREFIX+"a")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new Resource(DefaultRule.CONTEXT_PREFIX+"b")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new Resource("http://yyy.com")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new Resource("http://zzz.com")},
		new Node[]{ new BNode("a"), new BNode("d"), new BNode("e"), new Resource("http://aaa.com")},
		new Node[]{ new BNode("a"), new BNode("d"), new BNode("e"), new Resource(DefaultRule.CONTEXT_PREFIX+"d")},
		new Node[]{ new BNode("a"), new BNode("d"), new BNode("f"), new Resource(DefaultRule.CONTEXT_PREFIX+"e")},
		new Node[]{ new BNode("a"), new BNode("d"), new BNode("g"), new Resource(DefaultRule.CONTEXT_PREFIX+"f")},
		new Node[]{ new BNode("a"), new BNode("d"), new BNode("g"), new Resource(DefaultRule.CONTEXT_PREFIX+"g")},
		new Node[]{ new BNode("h"), new BNode("i"), new BNode("j"), new Resource(DefaultRule.CONTEXT_PREFIX+"h")},
		new Node[]{ new BNode("h"), new BNode("i"), new BNode("j"), new Resource("http://zzz.com")},
		new Node[]{ new BNode("h"), new BNode("k"), new BNode("l"), new Resource("http://aaa.com")},
		new Node[]{ new BNode("h"), new BNode("k"), new BNode("m"), new Resource(DefaultRule.CONTEXT_PREFIX+"g")}
	};
	
	private static final Node[][] UNIQUE_DATA = {
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new Resource("http://aaa.com")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new Resource("http://bbb.com")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new Resource("http://yyy.com")},
		new Node[]{ new BNode("a"), new BNode("b"), new BNode("c"), new Resource("http://zzz.com")},
		new Node[]{ new BNode("a"), new BNode("d"), new BNode("e"), new Resource("http://aaa.com")},
		new Node[]{ new BNode("a"), new BNode("d"), new BNode("f"), new Resource(DefaultRule.CONTEXT_PREFIX+"e")},
		new Node[]{ new BNode("a"), new BNode("d"), new BNode("g"), new Resource(DefaultRule.CONTEXT_PREFIX+"f")},
		new Node[]{ new BNode("h"), new BNode("i"), new BNode("j"), new Resource("http://zzz.com")},
		new Node[]{ new BNode("h"), new BNode("k"), new BNode("l"), new Resource("http://aaa.com")},
		new Node[]{ new BNode("h"), new BNode("k"), new BNode("m"), new Resource(DefaultRule.CONTEXT_PREFIX+"g")}
	};
	
	@Test
	public void testRDFSTemplateRulesLUBM() throws Exception{
		TreeSet<Node[]> sorted = new TreeSet<Node[]>(NodeComparator.NC);
		for(Node[] d:DATA)
			sorted.add(d);
		
		RemoveReasonedDupesIterator rrdi = new RemoveReasonedDupesIterator(sorted.iterator());
		
		int i=0;
		TreeSet<Node[]> no_dupes = new TreeSet<Node[]>(NodeComparator.NC);
		ArrayList<Node[]> no_dupes_us = new ArrayList<Node[]>();
		while(rrdi.hasNext()){
			Node[] next = rrdi.next();
			System.err.println(Nodes.toN3(next));
			no_dupes.add(next);
			no_dupes_us.add(next);
			i++;
		}
		
		assertTrue(no_dupes.size() == no_dupes_us.size());
		
		Iterator<Node[]> nditer = no_dupes.iterator();
		Iterator<Node[]> ndusiter = no_dupes_us.iterator();
		while(nditer.hasNext()){
			assertTrue(nditer.next().equals(ndusiter.next()));
		}
		
		TreeSet<Node[]> sorted_unique = new TreeSet<Node[]>(NodeComparator.NC);
		for(Node[] d:UNIQUE_DATA)
			sorted_unique.add(d);
		
		for(Node[] su:sorted_unique){
			assertTrue(no_dupes.contains(su));
		}
		
		for(Node[] nd:no_dupes){
			assertTrue(sorted_unique.contains(nd));
		}
	}
}
