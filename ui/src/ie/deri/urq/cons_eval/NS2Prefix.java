package ie.deri.urq.cons_eval;


import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.semanticweb.yars.nx.namespace.Namespace;

/**
 * 
 * @author Juergen Umbrich
 *
 */
public class NS2Prefix {
	private URI 	lookupFile;
	private Map<String,String>ns2PrefixMap = new TreeMap<String, String>();
	
	public NS2Prefix() {
		init();
	}
	
	private void init() {
		if(lookupFile!=null) return;
		
		Scanner s = null;
		try {
			lookupFile = new URI("http://prefix.cc/popular/all.txt.plain");
			 s = new Scanner(lookupFile.toURL().openStream());
			String [] pair;
			while(s.hasNextLine()){
				pair = s.nextLine().trim().split("\t"); 
				if(!pair[1].equals("???")){
					ns2PrefixMap.put(pair[1], pair[0]);
				}
			}
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(s!= null) s.close();
		}
	}
	
	public String renameNamespace(URL url){
		return renameNamespace(url.toExternalForm());
	}
	public String renameNamespace(String url){
		String ns = Namespace.getNamespace(url);
		
		if(ns != null && ns2PrefixMap.containsKey(ns)){
			url = url.replaceFirst(ns, ns2PrefixMap.get(ns)+":");
		}
		return url;
	}
	
	public static void main(String[] args) {
		NS2Prefix p = new NS2Prefix();
		String s="http://xmlns.com/foaf/0.1/test";
		System.err.println(p.renameNamespace(s));
	}
}
