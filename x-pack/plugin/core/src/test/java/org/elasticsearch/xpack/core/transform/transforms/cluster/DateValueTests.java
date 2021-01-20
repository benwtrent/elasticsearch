/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.cluster;

import java.io.IOException;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.transforms.AbstractSerializingTransformTestCase;

public class DateValueTests extends AbstractSerializingTransformTestCase<DateValue> {

    static DateValue randomDateValue() {
        long min = randomNonNegativeLong();
        long max = randomLongBetween(min, Math.max(min + randomNonNegativeLong(), min + 10));
        return new DateValue(min, max, 3600L);

    }

    public void testShouldPrune() {
        DateValue dateValue = new DateValue(0, 10, 10);

        assertFalse(dateValue.shouldPrune(new DateValue(20, 20, 10)));
        assertTrue(dateValue.shouldPrune(new DateValue(30, 30, 10)));
    }

    @Override
    protected DateValue doParseInstance(XContentParser parser) throws IOException {
        return DateValue.fromXContent(parser, false);
    }

    @Override
    protected Writeable.Reader<DateValue> instanceReader() {
        return DateValue::new;
    }

    @Override
    protected DateValue createTestInstance() {
        return randomDateValue();
    }
}
