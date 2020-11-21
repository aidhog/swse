package org.semanticweb.swse.cli;

import java.lang.reflect.Method;

import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Class for running command line tasks
 * 
 * @author Aidan Hogan
 */
public class Main {
	private static final String USAGE = "usage: org.semanticweb.rmi.cli.Main <utility> [options...]";
	private static final String PREFIX = "org.semanticweb.rmi.cli.";

	/**
	 * Main method
	 * @param args Command line args, first of which is the utility to run
	 */
	public static void main(String[] args) {
		try {
			if (args.length < 1) {
				StringBuffer sb = new StringBuffer();
				sb.append("missing <utility> arg where <utility> one of");
				sb.append("\n\tAbortRemoteCrawl  Abort a remote crawl");
				sb.append("\n\tRunRemoteConsolidation  Start remote consolidation");
				sb.append("\n\tRunRemoteCrawl  Start a remote crawl");
				sb.append("\n\tRunRemoteRanking  Start remote ranking");
				sb.append("\n\tRunRemoteReasoning  Start remote reasoning");
				sb.append("\n\tStartRemoteConsolidationServer  Set up RMI-controllable consolidator");
				sb.append("\n\tStartRemoteCrawlServer  Set up RMI-controllable crawler");
				sb.append("\n\tStartRemoteRankingServer  Set up RMI-controllable ranker");
				sb.append("\n\tStartRemoteReasonerServer  Set up RMI-controllable reasoner");
				
				usage(sb.toString());
			}


			Class<? extends Object> cls = Class.forName(PREFIX + args[0]);

			Method mainMethod = cls.getMethod("main", new Class[] { String[].class });

			String[] mainArgs = new String[args.length - 1];
			System.arraycopy(args, 1, mainArgs, 0, mainArgs.length);

			long time = System.currentTimeMillis();
			
			NxParser.DEFAULT_PARSE_DTS = false;
			mainMethod.invoke(null, new Object[] { mainArgs });

			long time1 = System.currentTimeMillis();

			System.err.println("time elapsed " + (time1-time) + " ms");
		} catch (Throwable e) {
			e.printStackTrace();
			usage(e.toString());
		}
	}

	private static void usage(String msg) {
		System.err.println(USAGE);
		System.err.println(msg);
		System.exit(-1);
	}
}
