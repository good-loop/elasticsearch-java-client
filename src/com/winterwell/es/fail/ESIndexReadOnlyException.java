package com.winterwell.es.fail;

import com.winterwell.es.ESPath;
import com.winterwell.web.WebEx;

/**
 * Probably low disk space + ES auto handling
 * @author daniel
 *
 */
public class ESIndexReadOnlyException extends WebEx.E404 implements IElasticException {

	public ESIndexReadOnlyException(WebEx ex) {
		super(null, "This error may be caused by low disk space vs  ES's flood stage watermark config. See: https://stackoverflow.com/questions/50609417/elasticsearch-error-cluster-block-exception-forbidden-12-index-read-only-all Details: "+ex.getMessage());
	}

	private static final long serialVersionUID = 1L;

}
