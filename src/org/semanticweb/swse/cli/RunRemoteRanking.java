package org.semanticweb.swse.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.deri.idrank.RankGraph;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.rank.RMIRankingConstants;
import org.semanticweb.swse.rank.SlaveRankingArgs;
import org.semanticweb.swse.rank.master.MasterRanker;
import org.semanticweb.swse.rank.master.MasterRankingArgs;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import com.ontologycentral.ldspider.queue.memory.Redirects;

/**
 * Main method to conduct a distributed crawl using remote crawlers 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunRemoteRanking {
	private final static Logger _log = Logger.getLogger(RunRemoteRanking.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option innaO = new Option("inna", "remote input file for named authority ranking, can use % delimiter");
		innaO.setArgs(1);
		innaO.setRequired(true);
		options.addOption(innaO);
		
		Option gznaO = new Option("gzna", "flag stating that input named authority file is gzipped");
		gznaO.setArgs(0);
		options.addOption(gznaO);
		
		Option inidO = new Option("inid", "remote input file for identifier ranking, can use % delimiter");
		inidO.setArgs(1);
		inidO.setRequired(true);
		options.addOption(inidO);
		
		Option gzidO = new Option("gzid", "flag stating that input identifier file is gzipped");
		gzidO.setArgs(0);
		options.addOption(gzidO);
		
		Option floodO = new Option("flood", "flag for flooding final results to all machines (unstable -- if you know why, please contact me)");
		floodO.setArgs(0);
		options.addOption(floodO);
		
		Option serversO = new Option("srvs", "servers.dat file");
		serversO.setArgs(1);
		serversO.setRequired(true);
		options.addOption(serversO);
		
		Option redirO = new Option("redirs", "local redirects file");
		redirO.setArgs(1);
		redirO.setRequired(true);
		options.addOption(redirO);
		
		Option gzredO = new Option("gzred", "redirects file is gzipped");
		gzredO.setArgs(0);
		gzredO.setRequired(false);
		options.addOption(gzredO);
		
		Option itersO = new Option("iters", "number of pagerank iterations (int)");
		itersO.setArgs(1);
		itersO.setRequired(false);
		options.addOption(itersO);
		
		Option dO = new Option("d", "damping factor (float)");
		dO.setArgs(1);
		dO.setRequired(false);
		options.addOption(dO);
		
		Option pldO = new Option("plds", "run pld level ranking");
		pldO.setArgs(0);
		pldO.setRequired(false);
		options.addOption(pldO);
		
		Option tboxO = new Option("tbox", "include tbox ranking");
		tboxO.setArgs(0);
		tboxO.setRequired(false);
		options.addOption(tboxO);
		
		Option outO = new Option("out", "remote/local output dir, can use a % delimiter");
		outO.setArgs(1);
		outO.setRequired(true);
		options.addOption(outO);
		
		Option helpO = new Option("h", "print help");
		options.addOption(helpO);

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e) {
			System.err.println("***ERROR: " + e.getClass() + ": " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		if (cmd.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		boolean gzNa = cmd.hasOption("gzna"), 
				gzId = cmd.hasOption("gzid"), 
				plds = cmd.hasOption("plds"), 
				tbox = cmd.hasOption("tbox"),
				gzred = cmd.hasOption("gzred"),
				flood = cmd.hasOption("flood");
		int iters = RankGraph.ITERATIONS;
		float d = RankGraph.DAMPING;
		
		if(cmd.hasOption("d")){
			d = Float.parseFloat(cmd.getOptionValue("d"));
		}
		if(cmd.hasOption("iters")){
			iters = Integer.parseInt(cmd.getOptionValue("iters"));
		}
		
		String inNa = cmd.getOptionValue("inna");
		String inId = cmd.getOptionValue("inid");
		
		String out = cmd.getOptionValue("out");
		String redirs = cmd.getOptionValue("redirs");
		
		
		RMIRegistries servers = new RMIRegistries(new File(cmd.getOptionValue("srvs")), RMIRankingConstants.DEFAULT_RMI_PORT);
		
		runRemoteRanking(inNa, gzNa, inId, gzId, servers, redirs, gzred, iters, d, plds, tbox, out, flood);
	}

	public static void runRemoteRanking(String inna, boolean gzinna, String inid, boolean gzinid, RMIRegistries servers, String redirects, boolean gzred, int iters, float d, boolean plds, boolean tbox, String out, boolean flood) throws Exception {
		SlaveRankingArgs sra = new SlaveRankingArgs(inna, inid, redirects, out);
		
		if(flood){
			sra.setOutFinal(SlaveRankingArgs.getDefaultRanksOut(out));
		}
		
		sra.setGzRedirects(gzred);
		sra.setGzInId(gzinid);
		sra.setGzInNa(gzinna);
		sra.setTboxId(tbox);
		sra.setTboxNa(tbox);
		sra.setPLD(plds);
		
		MasterRankingArgs mra = new MasterRankingArgs(MasterRankingArgs.getDefaultRanksOut(out), sra);
		mra.setDamping(d);
		mra.setIterations(iters);
		
		MasterRanker mr = new MasterRanker();
		mr.startRemoteTask(servers, RMIRankingConstants.DEFAULT_STUB_NAME, mra);
	}

	/**
	 * 
	 * @throws IOException 
	 * @throws ParseException 
	 * @throws URISyntaxException 
	 */
	public static Redirects readRedirects(String redirs, boolean gzipped) throws IOException, ParseException, URISyntaxException {
		InputStream is = new FileInputStream(redirs);
		if(gzipped){
			is = new GZIPInputStream(is);
		}
		
		NxParser nxp = new NxParser(is);
		Redirects r = new Redirects();
		int c =0;
		while(nxp.hasNext()){
			c++;
			Node[] next = nxp.next();
			r.put(new URI(next[0].toString()), new URI(next[1].toString()));
		}
		_log.info("Added "+c+" redirects...");
		is.close();
		return r;
	}
}
