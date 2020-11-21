package org.semanticweb.swse.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.semanticweb.nxindex.ScanIterator;
import org.semanticweb.nxindex.block.NodesBlockReaderNIO;
import org.semanticweb.saorr.auth.RedirectsAuthorityInspector;
import org.semanticweb.saorr.auth.redirs.FileRedirects;
import org.semanticweb.swse.lucene.utils.LuceneIndexBuilder;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class RunLocalLuceneBuild {
	private final static Logger _log = Logger.getLogger(RunLocalLuceneBuild.class.getSimpleName());
	
	public static void main(String args[]) throws Exception{
		Options options = new Options();
		
		Option inO = new Option("in", "local nxz input file");
		inO.setArgs(1);
		inO.setRequired(true);
		options.addOption(inO);
		
		Option redirO = new Option("redirs", "local redirects file");
		redirO.setArgs(1);
		redirO.setRequired(true);
		options.addOption(redirO);
		
		Option gzredO = new Option("gzred", "redirects file is gzipped");
		gzredO.setArgs(0);
		gzredO.setRequired(false);
		options.addOption(gzredO);
		
		Option ranksO = new Option("ranks", "local ranks nq.gz file");
		ranksO.setArgs(1);
		ranksO.setRequired(true);
		options.addOption(ranksO);
		
		Option outO = new Option("out", "lucene output dir");
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
		
		boolean gzred = cmd.hasOption("gzred");
		
		String in = cmd.getOptionValue("in");
		String ranks = cmd.getOptionValue("ranks");
		String out = cmd.getOptionValue("out");
		
		File dir = new File(cmd.getOptionValue("out"));
		dir.mkdirs();
		
		FileRedirects fr = readRedirects(cmd.getOptionValue("redirs"), gzred);
		
		runLocalLuceneBuild(in, ranks, fr, out);
	}

	public static void runLocalLuceneBuild(String in, String ranks, FileRedirects r, String out) throws Exception {
		NodesBlockReaderNIO nbr = new NodesBlockReaderNIO(in);
		ScanIterator si = new ScanIterator(nbr);
		
		InputStream is = new GZIPInputStream(new FileInputStream(ranks));
		NxParser nxp = new NxParser(is);
		
		RedirectsAuthorityInspector rai = new RedirectsAuthorityInspector(r);
		
		LuceneIndexBuilder.buildLucene(si, nxp, rai, out);
		
		is.close();
		nbr.close();
	}

	/**
	 * 
	 * @throws IOException 
	 * @throws ParseException 
	 * @throws URISyntaxException 
	 */
	public static FileRedirects readRedirects(String redirs, boolean gzipped) throws IOException, ParseException, URISyntaxException {
		InputStream is = new FileInputStream(redirs);
		if(gzipped){
			is = new GZIPInputStream(is);
		}
		
		FileRedirects r = FileRedirects.createFileRedirects(is);
		_log.info("Added "+r+" redirect pairs...");
		is.close();
		return r;
	}
}
