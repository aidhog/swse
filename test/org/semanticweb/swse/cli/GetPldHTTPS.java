package org.semanticweb.swse.cli;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.semanticweb.swse.cli.GetSomeNiceSPOCStats;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.tld.TldManager;

public class GetPldHTTPS {
	public static void main(String[] args) throws IOException, URISyntaxException {
		ArrayList<Resource> tests = new ArrayList<Resource>();
		
		tests.add(new Resource("https://wikidata.org/asdk"));
		tests.add(new Resource("https://wikidata.databox.me/asdk"));
		tests.add(new Resource("https://wikidata.natanael.arndt.xyz/asdk"));
		tests.add(new Resource("https://wikidata.ericp.solidtest.space/asdk"));
		tests.add(new Resource("https://wikidata.melvin.solid.live/asdk"));
		tests.add(new Resource("https://wikidata.learmonth.me/asdk"));
		
		for(Resource test:tests) {
			System.out.println(GetSomeNiceSPOCStats.getPLD(test));
		}
		
		TldManager tldm = new TldManager();
		
		for(Resource test:tests) {
			System.out.println(tldm.getPLD(new URI(test.toString())));
		}
	}
}
