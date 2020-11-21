package org.semanticweb.swse.ann.rank.utils;

import java.util.TreeSet;

import junit.framework.TestCase;

import org.junit.Test;
import org.semanticweb.swse.ann.rank.utils.ExtractGraphIterator;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.NodeComparator.NodeComparatorArgs;
import org.semanticweb.yars.nx.namespace.RDF;

public class ExtractGraphTest extends TestCase{
	private static final Node[][] DATA = {
		new Node[]{ new Resource("http://auri.com/"), new Resource("http://bturi.com/"), new Resource("http://curi.com/"), new Resource("http://1uri.com/")},
		new Node[]{ new Resource("http://1uri.com/"), new Resource("http://bturi.com/"), new Resource("http://1uri.com/"), new Resource("http://1uri.com/")},
		new Node[]{ new Resource("http://buri.com/"), RDF.TYPE, new Resource("http://curi.com/"), new Resource("http://2uri.com/")},
		new Node[]{ new Resource("http://curi.com/"), new Resource("http://bturi.com/"), new BNode("asdf"), new Resource("http://3uri.com/")},
		new Node[]{ new Resource("http://guri.com/"), new Resource("http://dturi.com/"), new Literal("gfds"), new Resource("http://3uri.com/")},
		new Node[]{ new Resource("http://huri.com/#frag"), new Resource("http://eturi.com/#frag"), new Resource("http://muri.com/"), new Resource("http://4uri.com/")},
		new Node[]{ new BNode("gfds"), new Resource("http://xturi.com/"), new BNode("asdfa"), new Resource("http://4uri.com/")},
		new Node[]{ new BNode("gfds"), new Resource("http://fturi.com/"), new BNode("asdfsag"), new Resource("http://5uri.com/")},
		new Node[]{ new Resource("http://iuri.com/"), new Resource("http://fturi.com/"), new Resource("http://nuri.com/"), new Resource("http://5uri.com/")},
		new Node[]{ new Resource("http://zuri.com/"), new Resource("http://zuri.com/"), new Resource("http://zuri.com/"), new Resource("http://6uri.com/")}
	};
	
	private static final Node[][] NO_TBOX_GRAPH = {
		new Node[]{ new Resource("http://1uri.com/"), new Resource("http://auri.com/")},
		new Node[]{ new Resource("http://1uri.com/"), new Resource("http://curi.com/")},
		
		new Node[]{ new Resource("http://2uri.com/"), new Resource("http://buri.com/")},
		
		new Node[]{ new Resource("http://3uri.com/"), new Resource("http://curi.com/")},
		new Node[]{ new Resource("http://3uri.com/"), new Resource("http://guri.com/")},
		
		new Node[]{ new Resource("http://4uri.com/"), new Resource("http://huri.com/")},
		new Node[]{ new Resource("http://4uri.com/"), new Resource("http://muri.com/")},
		
		new Node[]{ new Resource("http://5uri.com/"), new Resource("http://iuri.com/")},
		new Node[]{ new Resource("http://5uri.com/"), new Resource("http://nuri.com/")},
		
		new Node[]{ new Resource("http://6uri.com/"), new Resource("http://zuri.com/")},
		
	};
	
	private static final Node[][] TBOX_GRAPH = {
		new Node[]{ new Resource("http://1uri.com/"), new Resource("http://auri.com/")},
		new Node[]{ new Resource("http://1uri.com/"), new Resource("http://bturi.com/")},
		new Node[]{ new Resource("http://1uri.com/"), new Resource("http://curi.com/")},
		
		new Node[]{ new Resource("http://2uri.com/"), new Resource("http://buri.com/")},
		new Node[]{ new Resource("http://2uri.com/"), new Resource("http://curi.com/")},
		new Node[]{ new Resource("http://2uri.com/"), new Resource(RDF.NS.substring(0, RDF.NS.length()-1))},
		
		new Node[]{ new Resource("http://3uri.com/"), new Resource("http://bturi.com/")},
		new Node[]{ new Resource("http://3uri.com/"), new Resource("http://curi.com/")},
		new Node[]{ new Resource("http://3uri.com/"), new Resource("http://dturi.com/")},
		new Node[]{ new Resource("http://3uri.com/"), new Resource("http://guri.com/")},
		
		new Node[]{ new Resource("http://4uri.com/"), new Resource("http://eturi.com/")},
		new Node[]{ new Resource("http://4uri.com/"), new Resource("http://huri.com/")},
		new Node[]{ new Resource("http://4uri.com/"), new Resource("http://muri.com/")},
		new Node[]{ new Resource("http://4uri.com/"), new Resource("http://xturi.com/")},
		
		new Node[]{ new Resource("http://5uri.com/"), new Resource("http://fturi.com/")},
		new Node[]{ new Resource("http://5uri.com/"), new Resource("http://iuri.com/")},
		new Node[]{ new Resource("http://5uri.com/"), new Resource("http://nuri.com/")},
		
		new Node[]{ new Resource("http://6uri.com/"), new Resource("http://zuri.com/")},
	};
	
	@Test
	public void testAggregateTriples() throws Exception{
		NodeComparatorArgs nca = new NodeComparatorArgs();
		nca.setOrder(new int[]{3,0,1,2});
		NodeComparator nc = new NodeComparator(nca);
		
		TreeSet<Node[]> sorted = new TreeSet<Node[]>(nc);
		for(Node[] d:DATA)
			sorted.add(d);
		
		ExtractGraphIterator egint = new ExtractGraphIterator(sorted.iterator(), false);
		
		TreeSet<Node[]> sortedNoTbox = new TreeSet<Node[]>(NodeComparator.NC);
		while(egint.hasNext()){
			sortedNoTbox.add(egint.next());
		}
		
		System.err.println("Checking extract minus T-Box links");
		for(Node[] na:NO_TBOX_GRAPH){
			System.err.println(Nodes.toN3(na));
			assertTrue(sortedNoTbox.contains(na));
		}
		
		if(NO_TBOX_GRAPH.length != sortedNoTbox.size()){
			TreeSet<Node[]> expected = new TreeSet<Node[]>(NodeComparator.NC);
			for(Node[] d:NO_TBOX_GRAPH)
				expected.add(d);
			for(Node[] na:sortedNoTbox){
				System.err.println(Nodes.toN3(na));
				assertTrue(expected.contains(na));
			}
		}
		
		System.err.println("Checking extract plus T-Box links");
		ExtractGraphIterator egit = new ExtractGraphIterator(sorted.iterator(), true);
		
		TreeSet<Node[]> sortedTbox = new TreeSet<Node[]>(NodeComparator.NC);
		while(egit.hasNext()){
			sortedTbox.add(egit.next());
		}
		
		for(Node[] na:TBOX_GRAPH){
			System.err.println(Nodes.toN3(na));
			assertTrue(sortedTbox.contains(na));
		}
		
		if(TBOX_GRAPH.length != sortedTbox.size()){
			TreeSet<Node[]> expected = new TreeSet<Node[]>(NodeComparator.NC);
			for(Node[] d:TBOX_GRAPH)
				expected.add(d);
			for(Node[] na:sortedTbox){
				System.err.println(Nodes.toN3(na));
				assertTrue(expected.contains(na));
			}
		}
	}
}
