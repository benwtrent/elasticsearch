/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.ltr;

import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

public class RatedHit implements ToXContentObject {

    private final SearchHit hit;
    private final Integer rating;
    public RatedHit(SearchHit hit, Integer rating) {
        this.hit = hit;
        this.rating = rating;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        hit.toInnerXContent(builder, params);
        if (rating != null) {
            builder.field("_rating", rating);
        }
        builder.endObject();
        return builder;
    }
}
