package org.semanticweb.swse.qp.utils;

import java.util.ArrayList;

import junit.framework.TestCase;

import org.junit.Test;
import org.semanticweb.swse.qp.utils.QueryProcessor;
import org.semanticweb.yars.nx.Literal;

public class PickLiteralTest extends TestCase{
	private static final PickLiteralTestcase[] TESTS = {
		new PickLiteralTestcase(
			new Literal[]{new Literal("depiction") },
			new Literal[]{new Literal("Bild")},
			QueryProcessor.NULL_LANG,
			new Literal("depiction")
		)
	};
	
	@Test
	public void testPickLiteral() throws Exception{
		
		for(PickLiteralTestcase plt:TESTS){
			ArrayList<Literal> auth = new ArrayList<Literal>();
			for(Literal l:plt._auth)
				auth.add(l);
			
			ArrayList<Literal> nonauth = new ArrayList<Literal>();
			for(Literal l:plt._nonauth)
				nonauth.add(l);
			
			Literal l = QueryProcessor.chooseLabelOrComment(auth, nonauth, plt._lang);
			if(!l.equals(plt._answer)){
				System.err.println("expecting "+plt._answer.toN3()+" got "+l.toN3()+"\nTest -- "+plt);
			}
			assertTrue(l.equals(plt._answer));
		}
		
	}
	
	public static class PickLiteralTestcase{
		Literal[] _auth;
		Literal[] _nonauth;
		String _lang;
		Literal _answer;
		
		public PickLiteralTestcase(Literal[] auth, Literal[] nonauth, String lang, Literal answer){
			_auth = auth;
			_nonauth = nonauth;
			_lang = lang;
			_answer = answer;
		}
		
		public String toString(){
			StringBuilder sb = new StringBuilder();
			sb.append("auth: ");
			for(Literal a:_auth)
				sb.append(a.toN3()+" ");
			sb.append("non-auth: ");
			for(Literal na:_nonauth)
				sb.append(na.toN3()+" ");
			sb.append("lang: "+_lang);
			sb.append(" answer: "+_answer.toN3());
			return sb.toString();
		}
	}
}


