/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.List;

public abstract class PrefetchingFloatVectorValues extends FloatVectorValues implements HasIndexSlice {
    public abstract void prefetch(int ord) throws IOException;

    public abstract PrefetchingFloatVectorValues copy() throws IOException;

    public static PrefetchingFloatVectorValues fromFloats(List<float[]> vectors, int dim) {
        return new PrefetchingFloatVectorValues() {
            @Override
            public IndexInput getSlice() {
                return null;
            }

            @Override
            public void prefetch(int ord) {
                // all in memory, nothing to do
            }

            @Override
            public int size() {
                return vectors.size();
            }

            @Override
            public int dimension() {
                return dim;
            }

            @Override
            public float[] vectorValue(int targetOrd) {
                return vectors.get(targetOrd);
            }

            @Override
            public PrefetchingFloatVectorValues copy() {
                return this;
            }

            @Override
            public DocIndexIterator iterator() {
                return createDenseIterator();
            }
        };
    }
}
