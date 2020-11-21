package org.semanticweb.swse.ldspider.remote.utils;
//package org.semanticweb.rmi.ldspider.remote.utils;
//
//import java.net.URI;
//import java.util.logging.Logger;
//
//import org.apache.http.HttpEntity;
//
//import com.ontologycentral.ldspider.hooks.fetch.FetchFilter;
//import com.ontologycentral.ldspider.tld.TldManager;
//
///**
// * Maintains ratio of useful documents for PLDs
// * @author aidhog
// *
// */
//public class FetchFilterPLDInfo implements FetchFilter{
//	private static final Logger _log = Logger.getLogger(FetchFilter.class.getSimpleName());
//	private FetchFilter _ff;
//	private PldManager _pldm;
//	private TldManager _tldm;
//	
//	public FetchFilterPLDInfo(FetchFilter ff, PldManager pldm, TldManager tldm){
//		_ff = ff;
//		_pldm = pldm;
//		_tldm = tldm;
//	}
//	
//	public void setPldManager(PldManager pldm){
//		_pldm = pldm;
//	}
//	
//	public boolean fetchOk(URI u, int status, HttpEntity hen) {
//		boolean fetch = _ff.fetchOk(u, status, hen);
//		
//		if(!fetch)
//			_log.info("fetch filter denies "+u.toString()+" ct "+hen.getContentType());
//		try{
//			String pld = _tldm.getPLD(u);
//			if(pld!=null){
//				if(fetch){
//					_pldm.incrementUseful(pld);
//				} else{
//					_pldm.incrementUseless(pld);
//				}
//			}
//		} catch(Exception e){
//			;
//		}
//		
//		return fetch;
//	}
//
//}
