package com.winterwell.es.client;

import com.winterwell.es.client.admin.ClusterAdminClient;
import com.winterwell.es.client.admin.IndicesAdminClient;

/**
 * gateway to admin functions
 * @author daniel
 *
 */
public final class AdminClient {
	/**
	 * 
	 */
	private final ESHttpClient esHttpClient;

	
	/**
	 * @param esHttpClient
	 */
	AdminClient(ESHttpClient esHttpClient) {
		this.esHttpClient = esHttpClient;
	}

	public IndicesAdminClient indices() {
		return new IndicesAdminClient(this.esHttpClient);
	}

	public ClusterAdminClient cluster() {
		return new ClusterAdminClient(this.esHttpClient);
	}
	
}