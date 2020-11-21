package org.semanticweb.swse.ldspider.remote.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.Callback;

import com.ontologycentral.ldspider.hooks.error.ErrorHandler;
import com.ontologycentral.ldspider.queue.memory.Redirects;

/**
 * Follow all links (subject, predicate, object).
 * Maintains count per link
 * 
 * @author aharth, aidhog
 */
public class LinkFilter implements Callback{
	private static Logger _log = Logger.getLogger(LinkFilter.class.getName());
	private static String[] _suffix_blacklist = { ".txt", ".html", ".jpg", ".pdf", ".htm", ".png", ".jpg", ".jpeg", ".bmp", ".doc", ".xls", ".ppt", ".gif", ".exe", ".mp3", ".wma", ".ra", ".mp4", ".avi", ".qt", ".flv", ".mpg", ".mov", ".aac", ".wav", ".ps", ".tar", ".rar", ".gz", ".zip" };
	private static String[] _contains_blacklist = { "rossia.org/interests", "rossia.org/directory", "rossia.org/tools/tell",
		"rossia.org/tools/memadd", "rossia.org/tools/friends/", "rossia.org/tools/interface/",
		"rossia.org/tools/translate/", "d.opencalais.com/1/type/em/r/" 
	};
	
	Hashtable<String,Integer> _links;
	private Hashtable<String, TreeSet<String>> _linksPerDocument;
//	TreeSet<String> _local;
	ErrorHandler _eh;
	
	
	public LinkFilter(ErrorHandler eh) {
		_links = new Hashtable<String,Integer>();
		_eh = eh;
		_linksPerDocument = new Hashtable<String, TreeSet<String>>();
	}
	
	public Hashtable<String,Integer> getLinks() {
		return _links;
	}
	
	public void considerRedirects(Redirects r){
		ArrayList<String[]> redirs = new ArrayList<String[]>();
		for(Entry<String,Integer> e:_links.entrySet()){
			try{
				String re = r.getRedirect(new URI(e.getKey())).toString();
				if(!re.equals(e.getKey())){
					redirs.add(new String[]{e.getKey(), re});
				}
			} catch(Exception ex){
				;
			}
		}
		
		for(String[] redir:redirs){
			int c = _links.remove(redir[0]);
			addLink(redir[1],c);
		}
	}
	
	public void addLink(String link) {
		addLink(link, 1);
	}
	
	public void addLink(String link, int count) {
		increment(link, count);
	}

	public void startDocument() {
		;
	}
	
	public void endDocument() {
		;
	}
	
	private void increment(String link, int count){
		Integer i = _links.get(link);
		if(i==null){
			i=0;
		}
		_links.put(link, i+=count);
	}

	public void processStatement(Node[] nx) {
		for (int i = 0; i < nx.length-1; i++) {
			if (nx[i] instanceof Resource) {
				handleLink(nx[i].toString(), nx[nx.length-1].toString());
			}
		}
	}
	
	private void handleLink(String link, String context){
		try {
			URI norm = normalise(new URI(link));
			if(norm!=null){
				TreeSet<String> links = _linksPerDocument.get(context);
				if(links==null){
					links = new TreeSet<String>();
					_linksPerDocument.put(context, links);
				}
				if(links.add(link)){
					addLink(link);
				}
			}
		} catch (URISyntaxException e) {
			_log.info("ignoring invalid URI "+link+" from doc "+context);
		}
	}
	
	/**
	 * Removes fragment and then calls URI normalise function. 
	 * Returns null if URI is not valid for crawling.
	 * @param u
	 * @return
	 * @throws URISyntaxException
	 */
	public static URI normalise(URI u) throws URISyntaxException {
		if (u.getHost() == null) {
//			_log.info("no host in "+ u.toString()+", skipping");
			return null;
		}
		
		if (u.getScheme()==null || !(u.getScheme().equals("http"))) {
//			_log.info(u.getScheme() + " != http, skipping " + u);
			return null;
		}
		
		for (String suffix : _suffix_blacklist) {
			if(u.getPath()==null || u.getPath().isEmpty()){
//				_log.info(u +  " has no path, skipping");
				return null;
			}
			if (u.getPath().toLowerCase().endsWith(suffix)) {
//				_log.info(u +  " has blacklisted suffix " + suffix+", skipping");
				return null;
			}
		}
		
		for(String contains : _contains_blacklist){
			if(u.toString().toLowerCase().contains(contains.toLowerCase())){
				return null;
			}
		}
		
//		String path = u.getPath();
//		if (path == null || path.length() == 0) {
//			path = "/";
//		} 
//		else if (path.endsWith("/index.html")) {
//			path = path.substring(0, path.length()-10);
//		} else if (path.endsWith("/index.htm") || path.endsWith("/index.php") || path.endsWith("/index.asp")) {
//			path = path.substring(0, path.length()-9);
//		}
		
		URI noFrag = new URI(u.getScheme().toLowerCase(),
				u.getUserInfo(), u.getHost().toLowerCase(), u.getPort(),
				u.getPath(), u.getQuery(),
				null);
		URI norm = noFrag.normalize();
		
//		if(!norm.toString().equals(u.toString())){
//			_log.info("normalised "+u+" to "+norm);
//		}

		return norm;
	}
}
