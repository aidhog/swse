package org.semanticweb.swse.ann.reason.utils;

import org.semanticweb.saorr.auth.DefaultAuthorityInspector;
import org.semanticweb.yars.nx.Resource;

public class CheckAuthorityInspector extends DefaultAuthorityInspector {
	/**
	 * Constructor
	 */
	public CheckAuthorityInspector(){
		;
	}
	
	protected boolean checkAuthority(final Resource n, final Resource context) {
		throw new UnsupportedOperationException();
	}
	
	protected boolean checkAuthority(final String s, final String c) {
		throw new UnsupportedOperationException();
	}
}
