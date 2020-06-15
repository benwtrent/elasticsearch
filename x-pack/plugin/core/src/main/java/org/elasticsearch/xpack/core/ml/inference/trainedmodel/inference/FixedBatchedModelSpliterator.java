/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference;

import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

public class FixedBatchedModelSpliterator implements Spliterator<InferenceModel> {

    private final List<InferenceModel> models;
    private static final int batchSize = 64;
    private int index;        // current index, modified on advance/split
    private final int fence;  // one past last index
    private final int characteristics;

    public FixedBatchedModelSpliterator(List<InferenceModel> models) {
        this(models, 0, models.size());
    }

    public FixedBatchedModelSpliterator(List<InferenceModel> models, int origin, int fence) {
        this.models = models;
        this.index = origin;
        this.fence = fence;
        this.characteristics = Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.SIZED | Spliterator.SUBSIZED;
    }

    @Override
    public Spliterator<InferenceModel> trySplit() {
        int lo = index, mid = batchSize;
        return (lo >= mid)
            ? null
            : new FixedBatchedModelSpliterator(models, lo, index = mid);
    }

    @Override
    public void forEachRemaining(Consumer<? super InferenceModel> action) {
        List<InferenceModel> a; int i, hi; // hoist accesses and checks from loop
        if (action == null)
            throw new NullPointerException();
        if ((a = models).size() >= (hi = fence) &&
            (i = index) >= 0 && i < (index = hi)) {
            do { action.accept(a.get(i)); } while (++i < hi);
        }
    }

    @Override
    public boolean tryAdvance(Consumer<? super InferenceModel>  action) {
        if (action == null)
            throw new NullPointerException();
        if (index >= 0 && index < fence) {
            action.accept(models.get(index++));
            return true;
        }
        return false;
    }

    @Override
    public long estimateSize() { return (long)(fence - index); }

    @Override
    public int characteristics() {
        return characteristics;
    }

}

