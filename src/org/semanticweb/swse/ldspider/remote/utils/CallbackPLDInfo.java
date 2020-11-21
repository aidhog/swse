package org.semanticweb.swse.ldspider.remote.utils;
//package org.semanticweb.rmi.ldspider.crawler.remote;
//
//import java.net.URI;
//import java.util.Hashtable;
//import java.util.TreeSet;
//
//import org.semanticweb.yars.nx.Node;
//import org.semanticweb.yars.nx.Resource;
//import org.semanticweb.yars.nx.parser.Callback;
//
//import com.ontologycentral.ldspider.tld.TldManager;
//
///**
// * Maintains PLDInfo for scheduling
// * @author aidhog
// *
// */
//public class CallbackPLDInfo implements Callback{
//	PldManager _pldm;
//	TldManager _tldm;
//	long _delay;
//	double _polls;
//	
//	private Hashtable<String, TreeSet<String>> _pldsPerDocument;
//
//	public CallbackPLDInfo(PldManager pldm, TldManager tldm){
//		_pldm = pldm;
//		_tldm = tldm;
//		_pldsPerDocument = new Hashtable<String, TreeSet<String>>();
//	}
//
//	public PldManager getPldManager(){
//		return _pldm;
//	}
//
//	public void setPldManager(PldManager pldm){
//		_pldm = pldm;
//	}
//
//	public void endDocument() {
//		;
//	}
//
//	public void processStatement(Node[] nx) {
//		try{
//			URI cu = new URI(nx[nx.length-1].toString());
//			String c = cu.toString();
//			String pldC = _tldm.getPLD(cu);
//
//			if (pldC!=null) for (int i = 0; i < nx.length-1; i++) {
//				if (nx[i] instanceof Resource) {
//					URI u = new URI(nx[i].toString());
//					String pldN = _tldm.getPLD(u);
//
//					if(pldN!=null && !pldN.equals(pldC)){
//						handlePLD(pldN, c);
//					}
//
//				}
//			}
//		} catch(Exception e){
//			;
//		}
//	}
//	
//	private void handlePLD(String pld, String context){
//		TreeSet<String> plds = _pldsPerDocument.get(context);
//		if(plds==null){
//			plds = new TreeSet<String>();
//			_pldsPerDocument.put(context, plds);
//		}
//		if(plds.add(pld)){
//			_pldm.incrementInlinks(pld);
//		}
//	}
//
//	public void startDocument() {
//		;
//	}
//
//}
