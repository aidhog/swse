package org.semanticweb.swse.ldspider;

import java.io.IOException;
import java.net.URISyntaxException;

import com.ontologycentral.ldspider.http.ConnectionManager;
import com.ontologycentral.ldspider.tld.TldManager;

public class TldManagerBugTest {
	public static void main(String args[]) throws URISyntaxException, IOException{
		new TldManager(new ConnectionManager(null, 0, null, null, 2));
		new TldManager(new ConnectionManager(null, 0, null, null, 2));
	}
}
