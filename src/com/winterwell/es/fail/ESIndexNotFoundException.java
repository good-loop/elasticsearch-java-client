package com.winterwell.es.fail;

import com.winterwell.es.ESPath;
import com.winterwell.web.WebEx;

public class ESIndexNotFoundException extends WebEx.E404 implements IElasticException {

	public ESIndexNotFoundException(ESPath esPath) {
		super(null, "No index "+esPath);
	}

	private static final long serialVersionUID = 1L;

}
