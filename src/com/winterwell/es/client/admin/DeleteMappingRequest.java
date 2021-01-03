package com.winterwell.es.client.admin;

import com.winterwell.es.client.ESHttpClient;
import com.winterwell.es.client.ESHttpRequest;
import com.winterwell.es.client.IESResponse;

/**
 * Delete a mapping (schema) AND THE DATA.
 * @author daniel
 *
 */
public class DeleteMappingRequest extends ESHttpRequest<DeleteMappingRequest,IESResponse> {

	public DeleteMappingRequest(ESHttpClient hClient, String... indices) {
		super(hClient, "_mapping");
		setIndices(indices);		
		method = "DELETE";
	}


}
