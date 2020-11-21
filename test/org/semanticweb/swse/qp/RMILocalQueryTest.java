package org.semanticweb.swse.qp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.TestCase;

import org.apache.lucene.search.ScoreDoc;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;
import org.semanticweb.swse.qp.utils.QueryProcessor;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.RDFS;

public class RMILocalQueryTest extends TestCase{
	
	private static final String LUCENE = "testdata/index0/lucene/";
	
	private static final String SPOC = "testdata/index0/spoc.nqz";
	
	private static final String SPARSE = "testdata/index0/spoc.sparse.nqz";
	
	private static final String KEYWORDQ = "Andreas";
	
	private static final Resource FOCUSQ = new Resource("http://harth.org/andreas/foaf#blogs");
	
	private static final Resource ENTITYQ = RDFS.SEEALSO;
	
	private static final int FROM = 1;
	
	private static final int TO = 4;
	
	public static void main(String[] args) throws Exception{
		Logger log = Logger.getLogger(PldManager.class.getName());
		log.setLevel(Level.WARNING);
		
		QueryProcessor qp = new QueryProcessor(SPOC, SPARSE, LUCENE);
		ScoreDoc[] hits = qp.getDocs(KEYWORDQ, FROM, TO);
		
		System.out.println("=======================KEYWORD=======================");
		Iterator<Node[]> iter = qp.getSnippets(hits);
		
		while(iter.hasNext()){
			System.out.println(Nodes.toN3(iter.next()));
		}
		
		System.out.println("=======================FOCUS=======================");
		iter = qp.getFocus(FOCUSQ);
		
		while(iter.hasNext()){
			System.out.println(Nodes.toN3(iter.next()));
		}
		
		System.out.println("=======================FOCUS=======================");
		iter = qp.getFocus(FOCUSQ);
		
		while(iter.hasNext()){
			System.out.println(Nodes.toN3(iter.next()));
		}
		
		System.out.println("=======================ENTITY=======================");
		iter = qp.getEntity(ENTITYQ);
		
		while(iter.hasNext()){
			System.out.println(Nodes.toN3(iter.next()));
		}
		
		System.out.println("=======================ENTITIES=======================");
		ArrayList<Node> entities = new ArrayList<Node>();
		entities.add(new Resource("blah"));
		entities.add(ENTITYQ);
		iter = qp.getEntities(entities);
		
		while(iter.hasNext()){
			System.out.println(Nodes.toN3(iter.next()));
		}
	}
}
