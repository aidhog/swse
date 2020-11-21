package org.semanticweb.swse.cli;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.yars.stats.Count;

import com.ontologycentral.ldspider.tld.TldManager;

public class ExtractLogInformation {
	public static void main(String[] args) throws IOException{
		String suffix = "crawlstats/access.";
		String prefix = ".log";

		Logger log = Logger.getLogger("");
		log.setLevel(Level.SEVERE);

		Count<Integer> responseCode = new Count<Integer>();
		Count<String> plds = new Count<String>();

		Count<String> contentType200 = new Count<String>();
		int lookups = 0, RDF200 = 0;

		Count<String> pldsRedirs = new Count<String>();
		Count<String> plds200RDF = new Count<String>();

		TldManager tldm = new TldManager();

		int robots = 0;

		System.err.println("Reading new PLDs from log files...");
		
		HashMap<String,Long> newplds = new HashMap<String,Long>();
		HashSet<String> plds200rdfhs = new HashSet<String>();
		
		for(int i=0; i<8; i++){

			String file = suffix+i+prefix;

			System.err.println("Reading log file "+file+"...");

			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;

			long start = 0, last = 0;
			
			while((line=br.readLine())!=null){
				boolean r = false;
				try{
					StringTokenizer tok = new StringTokenizer(line);
					long ms = Long.parseLong(tok.nextToken());

					if(start==0){
						start = ms;
					} else{
						if(ms-last> (60*60)){
							System.err.println("Adjusting log by "+(ms-last)+" secs... or "+((ms-last)/(60*1000))+" mins");
							start+=ms-last;
						}
					}
					
					long since = ms-start;

					last = ms;
					

					//duration
					tok.nextToken();

					//IP
					tok.nextToken();

					//response code
					String rcs = tok.nextToken();
					int rc = Integer.parseInt(rcs.substring(8));

					//content length
					tok.nextToken();

					//method type (GET)
					tok.nextToken();

					//URI
					String u = tok.nextToken().toLowerCase();
					URI uri = null;
					try{
						uri = new URI(u);
						if(u.endsWith("robots.txt")){
							r = true;
						}
					} catch(URISyntaxException use){
						use.printStackTrace();
					}

					if(!r){
						String pld = null;
						if(uri!=null){
							try{
								pld = tldm.getPLD(uri);
							} catch(Exception e){
								System.err.println("Cannot parse pld from "+uri);
							}
						}

						if(pld==null)
							pld = "null";

						Long hr = newplds.get(pld);
						if(hr==null || hr>since)
							newplds.put(pld, since);
						
//						boolean newp = false;

//						String pld = null;
//						if(uri!=null){
//							try{
//								pld = tldm.getPLD(uri);
//							} catch(Exception e){
//								System.err.println("Cannot parse pld from "+uri);
//							}
//						}
//
//						if(pld==null)
//							pld = "null";
//						
//						Long hr = newplds.get(pld);
//						if(hr==since){
//							newp = true;
//						} else if(hr>since){
//							throw new RuntimeException("Error getting first instance of pld "+pld+" "+since+" "+hr);
//						}

						//-
						String h = tok.nextToken();
						if(!h.equals("-")){
							throw new RuntimeException(line);
						}

						// NONE/-
						tok.nextToken();

						//content type
						if(tok.hasMoreTokens()){
							String ct = tok.nextToken();
							if(rc==200){
								if(ct.equals("application/rdf+xml")){
									plds200rdfhs.add(pld);
								}
							}
						}
					}
				} catch(NoSuchElementException nsse){
					nsse.printStackTrace();
					System.err.println(line);
				}
				
				
			}
			br.close();
		}
		
		int totalPLDnonRDF = 0, totalPLDRDF = 0;
		int oldhour = 0, hour = 0;
		HashMap<Integer, CrawlStats> perHour = new HashMap<Integer, CrawlStats>();
		HashSet<String> seenPld = new HashSet<String>();
		for(int i=0; i<8; i++){

			String file = suffix+i+prefix;

			System.err.println("Reading log file "+file+"...");

			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;

			long start = 0;
			long last = 0;
			
			
			CrawlStats cs = null;
			
			while((line=br.readLine())!=null){
				boolean r = false;
				try{
					lookups++;

					StringTokenizer tok = new StringTokenizer(line);
					long ms = Long.parseLong(tok.nextToken());

					if(start==0){
						start = ms;
						hour = 1;
					} else{
						if(ms-last> (60*60)){
							System.err.println("Adjusting log by "+(ms-last)+" secs... or "+((ms-last)/(60*1000))+" mins");
							start+=ms-last;
						}
						hour = (int)Math.ceil((double)(ms - start)/(double)(60*60));
						if(hour==0)
							hour = 1;
					}
					
					if(hour!=oldhour){
						cs = perHour.get(hour);
						if(cs==null){
							cs = new CrawlStats();
							perHour.put(hour, cs);
						}
					}
					oldhour = hour;
					
					long since = ms-start;

					last = ms;

					//duration
					tok.nextToken();

					//IP
					tok.nextToken();

					//response code
					String rcs = tok.nextToken();
					int rc = Integer.parseInt(rcs.substring(8));

					//content length
					tok.nextToken();

					//method type (GET)
					tok.nextToken();

					//URI
					String u = tok.nextToken().toLowerCase();
					URI uri = null;
					try{
						uri = new URI(u);
						if(u.endsWith("robots.txt")){
							robots++;
							r = true;
						}
					} catch(URISyntaxException use){
						use.printStackTrace();
					}

					if(!r){
						cs.lookups++;
						
						boolean newp = false;
						responseCode.add(rc);

						String pld = null;
						if(uri!=null){
							try{
								pld = tldm.getPLD(uri);
							} catch(Exception e){
								System.err.println("Cannot parse pld from "+uri);
							}
						}

						if(pld==null)
							pld = "null";
						
						Long hr = newplds.get(pld);
						if(hr==since){
							if(seenPld.add(pld)){
								newp = true;
								cs.newplds++;
							}
						} else if(hr>since){
							throw new RuntimeException("Error getting first instance of pld "+pld+" "+since+" "+hr);
						}

						plds.add(pld);
						cs.plds.add(pld);
						
						if(plds200rdfhs.contains(pld)){
							totalPLDRDF++;
							if(newp){
								cs.newrdfplds++;
							}
							cs.plds200RDF.add(pld);
						} else totalPLDnonRDF++;

						//-
						String h = tok.nextToken();
						if(!h.equals("-")){
							throw new RuntimeException(line);
						}

						// NONE/-
						tok.nextToken();

						//content type
						if(tok.hasMoreTokens()){
							String ct = tok.nextToken();
							if(rc==200){
								contentType200.add(ct);
								if(ct.equals("application/rdf+xml")){
									plds200RDF.add(pld);
									RDF200++;
									cs.rdf200++;
								}
							} else if(rc>299 && rc<400){
								pldsRedirs.add(pld);
								cs.redirs++;
							} else{
								cs.other++;
							}
						} else{
							cs.other++;
						}
					}
				} catch(NoSuchElementException nsse){
					nsse.printStackTrace();
					System.err.println(line);
				}
			}
			br.close();
		}
		System.err.println("Found "+lookups+" lookups.");

		PrintStream ps = new PrintStream(new FileOutputStream("crawlstats/summary.stats"));
		ps.println("all-lookups "+lookups);
		ps.println("robots "+robots);
		ps.println("plds "+plds.size());
		ps.println("plds-200/RDF "+plds200RDF.size());
		ps.println("plds-200/RDF-total-lookups "+totalPLDRDF);
		ps.println("plds-nonRDF-total-lookups "+totalPLDnonRDF);
		
		ps.close();

		ps = new PrintStream(new FileOutputStream("crawlstats/200rdf.pld.stats"));
		plds200RDF.printOrderedStats(ps);
		ps.close();

		ps = new PrintStream(new FileOutputStream("crawlstats/redirs.pld.stats"));
		pldsRedirs.printOrderedStats(ps);
		ps.close();

		ps = new PrintStream(new FileOutputStream("crawlstats/ct200.stats"));
		contentType200.printOrderedStats(ps);
		ps.close();

		ps = new PrintStream(new FileOutputStream("crawlstats/rc.stats"));
		responseCode.printOrderedStats(ps);
		ps.close();

		ps = new PrintStream(new FileOutputStream("crawlstats/plds.stats"));
		plds.printOrderedStats(ps);
		ps.close();
		
		System.err.println("Hour "+hour);
		
		PrintStream ps1 = new PrintStream(new FileOutputStream("crawlstats/ph.200rdf.stats"));
		PrintStream ps2 = new PrintStream(new FileOutputStream("crawlstats/ph.redirs.stats"));
		PrintStream ps3 = new PrintStream(new FileOutputStream("crawlstats/ph.other.stats"));
		PrintStream ps4 = new PrintStream(new FileOutputStream("crawlstats/ph.plds.stats"));
		PrintStream ps5 = new PrintStream(new FileOutputStream("crawlstats/ph.newplds.stats"));
		PrintStream ps6 = new PrintStream(new FileOutputStream("crawlstats/ph.rdfplds.stats"));
		PrintStream ps7 = new PrintStream(new FileOutputStream("crawlstats/ph.newrdfplds.stats"));
		PrintStream ps8 = new PrintStream(new FileOutputStream("crawlstats/ph.lookups.stats"));
		PrintStream ps9 = new PrintStream(new FileOutputStream("crawlstats/ph.breakdown.stats"));
		for(int i=1; i<=hour; i++){
			CrawlStats cs = perHour.get(i);
			if(cs==null){
				System.err.println("No entry for hour "+i);
			}
			ps1.println(i+" "+cs.rdf200);
			ps2.println(i+" "+cs.redirs);
			ps3.println(i+" "+cs.other);
			ps4.println(i+" "+cs.plds.size());
			ps5.println(i+" "+cs.newplds);
			ps6.println(i+" "+cs.plds200RDF.size());
			ps7.println(i+" "+cs.newrdfplds);
			ps8.println(i+" "+cs.lookups);
			
			int ls = cs.rdf200 + cs.redirs + cs.other;
			
			ps9.println(i+" "+(double)cs.rdf200/(double)ls+" "+(double)cs.redirs/(double)ls+" "+(double)cs.other/(double)ls+" ");
		}
		ps1.close();
		ps2.close();
		ps3.close();
		ps4.close();
		ps5.close();
		ps6.close();
		ps7.close();
		ps8.close();
		ps9.close();
	}
	
	public static class CrawlStats{
		Count<String> plds = new Count<String>();
		Count<String> plds200RDF = new Count<String>();

		int lookups = 0, newplds = 0, newrdfplds = 0, rdf200 = 0, redirs = 0, other = 0;

		
		public CrawlStats(){
			;
		}
	}
}
