package org.wuzl.util.httpclient;

import org.junit.Test;

public class PooledHttpClientAdaptorTest {
	@Test
	public void test() {
		System.out.println(PooledHttpClientAdaptor.getDefaultPooledHttpClientAdaptor().doGet("http://www.baidu.com"));
	}
}
