/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;

/**
 * Custom function state that is supplied to the transform functions while they are running.
 *
 * The state is stored along side the overall transform state.
 */
public interface FunctionState extends NamedWriteable, ToXContentObject {
}
