package com.winterwell.es.client;


/**
 * @see org.DeleteByQueryRequest.action.deletebyquery.DeleteByQueryRequestBuilder
 * @author daniel
 *
 */
public class DeleteByQueryRequest extends ESHttpRequest<DeleteByQueryRequest,IESResponse> {


	public DeleteByQueryRequest(ESHttpClient hClient, String index) {
		super(hClient, "_query");
		method = "DELETE";
		setIndex(index);
	}
	
    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
	public DeleteByQueryRequest setTypes(String... types) {
		assert types.length==1 : "TODO";
		setType(types[0]);
		return this;
	}

	public DeleteByQueryRequest setFrom(int i) {
		params.put("from", i);
		return this;
	}
	
}
