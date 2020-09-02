package com.winterwell.es;

import org.junit.Test;

import com.winterwell.es.client.ESHttpClient;
import com.winterwell.utils.Printer;

public class ESTypeTest extends ESTest {
	
	@Test
	public void testNoIndex_smoke() {
		ESHttpClient ec = new ESHttpClient();
		String ests = new ESType().text().noIndex().toString();
		Printer.out(ests);
	}

}
