package com.winterwell.es.fail;

import java.util.List;

/**
 * 
 * Note: The first exception is picked as the base one.
 * @author daniel
 *
 */
public class ESBulkException extends ESException {
	
	private List<Exception> errors;

	public List<Exception> getErrors() {
		return errors;
	}
	
	public ESBulkException(List<Exception> exs) {
		super(exs.size()+" errors - call getErrors() for details: "+exs);
		this.errors = exs;
	}

	private static final long serialVersionUID = 1L;

}
