package com.winterwell.es.client.admin;

import com.winterwell.es.client.ESHttpClient;

public class ClusterAdminClient {

	private ESHttpClient hClient;

	public ClusterAdminClient(ESHttpClient esHttpClient) {
		this.hClient = esHttpClient;
	}
	
	/**
	 * Override the safety feature to allow low-hard-drive usage.
	 * Use-case: On a dev machine, this can be an annoyance. E.g. a 100mb ES node can refuse to run
	 * as it "only" has 5gb of memory on a 200gb laptop.
	 * 
	 * https://stackoverflow.com/questions/50609417/elasticsearch-error-cluster-block-exception-forbidden-12-index-read-only-all
	 * @return 
	 */
	public ClusterOverridReadOnlyRequest prepareOverrideReadOnly() {
		return new ClusterOverridReadOnlyRequest(hClient);
	}


}
