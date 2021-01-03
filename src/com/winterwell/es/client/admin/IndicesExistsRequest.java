package com.winterwell.es.client.admin;

import com.winterwell.es.client.ESHttpRequest;
import com.winterwell.es.client.IESResponse;

/**
 * @testedby IndicesExistsRequestBuilderTest
 * @author daniel
 *
 */
public class IndicesExistsRequest extends ESHttpRequest<IndicesExistsRequest,IESResponse> {

	public IndicesExistsRequest(IndicesAdminClient iac) {
		super(iac.hClient, null);
		method = "HEAD";
	}

}
