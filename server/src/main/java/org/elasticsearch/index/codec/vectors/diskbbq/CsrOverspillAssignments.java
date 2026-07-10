/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

/**
 * A multi-valued {@link OverspillAssignments}: each vector ordinal may be overspilled to zero,
 * one, or many secondary centroids, stored in compressed-sparse-row form.
 * {@code values[offsets[ordinal]..offsets[ordinal + 1])} holds the secondary centroid ordinals
 * for {@code ordinal}.
 */
public record CsrOverspillAssignments(int[] offsets, int[] values) implements OverspillAssignments {

    public CsrOverspillAssignments {
        assert offsets.length > 0;
        assert offsets[offsets.length - 1] == values.length;
    }

    @Override
    public int size() {
        return offsets.length - 1;
    }

    @Override
    public PrimitiveIterator.OfInt getAssignmentsFor(int ordinal) {
        if (ordinal < 0 || ordinal >= size()) {
            return EMPTY_ITERATOR;
        }
        int start = offsets[ordinal];
        int end = offsets[ordinal + 1];
        if (start == end) {
            return EMPTY_ITERATOR;
        }
        return new CsrIterator(values, start, end);
    }

    private static class CsrIterator implements PrimitiveIterator.OfInt {
        private final int[] values;
        private final int end;
        private int pos;

        private CsrIterator(int[] values, int start, int end) {
            this.values = values;
            this.pos = start;
            this.end = end;
        }

        @Override
        public boolean hasNext() {
            return pos < end;
        }

        @Override
        public int nextInt() {
            if (pos >= end) {
                throw new NoSuchElementException();
            }
            return values[pos++];
        }
    }
}
