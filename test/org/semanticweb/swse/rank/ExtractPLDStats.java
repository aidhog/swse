package org.semanticweb.swse.rank;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.stats.Count;

public class ExtractPLDStats extends TestCase{
//	public void testExtractPlds() throws IOException{
//		String pld_data = "testdata/pld_links.dat";
//		
//		BufferedReader br = new BufferedReader(new FileReader(pld_data));
//		
//		String line;
//		
//		TreeSet<String> pldsA = new TreeSet<String>();
//		
//		int c = 0;
//		while((line=br.readLine())!=null){
//			StringTokenizer tok = new StringTokenizer(line, " ");
//			tok.nextToken();
//			tok.nextToken();
//			
//			String pldA = tok.nextToken();
//			float pldRA = Float.parseFloat(tok.nextToken());
//			
//			String pldB = tok.nextToken();
//			float pldRB = Float.parseFloat(tok.nextToken());
//			
//			pldsA.add(pldA);
////			plds.add(pldB);
//			
//			c++;
//		}
//		
//		br.close();
//		
//		br = new BufferedReader(new FileReader(pld_data));
//		
//		
//		TreeSet<String> pldsRDF = new TreeSet<String>();
//		int inlinks = 0;
//		
//		
//		while((line=br.readLine())!=null){
//			StringTokenizer tok = new StringTokenizer(line, " ");
//			tok.nextToken();
//			tok.nextToken();
//			
//			String pldA = tok.nextToken();
//			float pldRA = Float.parseFloat(tok.nextToken());
//
//			String pldB = tok.nextToken();
//			float pldRB = Float.parseFloat(tok.nextToken());
//			
//			if(pldsA.contains(pldB)){
//				inlinks++;
//			}
//			
//			pldsA.add(pldA);
////			plds.add(pldB);
//			
//			
//		}
//		
//		br.close();
//		
//		System.err.println(pldsA.size());
//		System.err.println(inlinks);
//		System.err.println(c);
//	}
	
	public void testAverageOutdegreeOfInlinks() throws IOException, ParseException{
		String pld_data = "testdata/pld_links.dat";
		
		BufferedReader br = new BufferedReader(new FileReader(pld_data));
		
		String line;
		
		TreeSet<String> pldsA = new TreeSet<String>();
		
		int c = 0;
		Resource node = new Resource("http://dbpedia.org/");
		
		while((line=br.readLine())!=null){
			StringTokenizer tok = new StringTokenizer(line, " ");
			tok.nextToken();
			tok.nextToken();
			
			String pldA = tok.nextToken();
			tok.nextToken();
//			float pldRA = Float.parseFloat(tok.nextToken());
			
			String pldB = tok.nextToken();
			tok.nextToken();
//			float pldRB = Float.parseFloat(tok.nextToken());
			
			if(node.equals(NxParser.parseResource(pldB)))
				pldsA.add(pldA);
//			plds.add(pldB);
			
			c++;
		}
		
		br.close();
		
		br = new BufferedReader(new FileReader(pld_data));
		
		int links = 0;
		
		Count<String> indegree = new Count<String>();
		
		while((line=br.readLine())!=null){
			StringTokenizer tok = new StringTokenizer(line, " ");
			tok.nextToken();
			tok.nextToken();
			
			String pldA = tok.nextToken();
			tok.nextToken();
//			float pldRA = Float.parseFloat(tok.nextToken());

			String pldB = tok.nextToken();
			tok.nextToken();
//			float pldRB = Float.parseFloat(tok.nextToken());
			
			if(pldsA.contains(pldA)){
				links++;
				indegree.add(pldA);
			}
			
//			pldsA.add(pldA);
//			plds.add(pldB);
//			
//			
		}
		
		System.err.println(pldsA);
		indegree.printOrderedStats();
		
		br.close();
		
		System.err.println(pldsA.size());
		System.err.println(links);
		System.err.println(c);
	}
	
//	public void testRanking() throws IOException, ParseException, URISyntaxException{
//		String pld_data = "testdata/pld_links.dat";
//		
//		BufferedReader br = new BufferedReader(new FileReader(pld_data));
//		
//		String line;
//		
//		TreeSet<Node[]> links = new TreeSet<Node[]>(NodeComparator.NC);
//		
//		int c = 0;
//		while((line=br.readLine())!=null){
//			StringTokenizer tok = new StringTokenizer(line, " ");
//			tok.nextToken();
//			tok.nextToken();
//			
//			String pldA = tok.nextToken().toLowerCase();
//			tok.nextToken();
//			
//			String pldB = tok.nextToken().toLowerCase();
//			tok.nextToken();
//			
//			links.add(new Node[]{NxParser.parseNode(pldA), NxParser.parseNode(pldB)});
////			plds.add(pldB);
//			
//			c++;
//		}
//		
//		br.close();
//		
//		ResetableIterator<Node[]> ri = new ResetableCollectionNodeArrayIterator(links);
//		PageRank pr = new PageRank(ri, 10, 0.85f);
//		
//		CallbackPageRank cpr = new CallbackPageRank();
//		pr.process(cpr);
//		
//		cpr.getMap();
//		
//		System.err.println(cpr.getMap().getMin());
//		System.err.println(cpr.getMap().get("http://dbpedia.org/"));
//		
//		TreeSet<Map.Entry<String, Float>> ranks = new TreeSet<Map.Entry<String, Float>>(new EntryComparator());
//		for(Map.Entry<String, Float> e:cpr.getMap().entrySet()){
//			ranks.add(e);
//		}
//		
//		int topk = 10, cnt = 0;
//		for(Map.Entry<String, Float> rank:ranks){
//			System.err.println(rank);
//			cnt++;
//			if(cnt==topk)
//				break;
//		}
//	}
	
	public static class EntryComparator implements Comparator<Map.Entry<String,Float>>{

		public int compare(Entry<String, Float> arg0, Entry<String, Float> arg1) {
			int comp = Float.compare(arg1.getValue(),arg0.getValue());
			if(comp!=0)
				return comp;
			return arg0.getKey().compareTo(arg1.getKey());
		}
		
	}
}
