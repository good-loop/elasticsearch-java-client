package com.winterwell.es.client;

import java.util.Arrays;
import java.util.List;

import com.winterwell.utils.StrUtils;

public class ClearScrollRequest extends ESHttpRequest<ClearScrollRequest, IESResponse> {

	public ClearScrollRequest(ESHttpClient esHttpClient) {
		super(esHttpClient, "_search/scroll");
		method = "DELETE";
	}

	public ClearScrollRequest setScrollIds(List<String> asList) {
		params.put("scroll_id", StrUtils.join(asList, ","));
		return this;
	}

	/**
	 * @deprecated Use {@link #setScrollIds(List)}
	 */
	@Override
	public ClearScrollRequest setId(String id) {
		throw new IllegalStateException("You want setScrollIds");
	}

	public ClearScrollRequest setScrollId(String scrollId) {
		return setScrollIds(Arrays.asList(scrollId));
	}
}
