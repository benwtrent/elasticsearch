/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.transform.utils;

import org.elasticsearch.cluster.metadata.Metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Yet Another String utilities class.
 */
public final class TransformStrings {

    /**
     * Valid user id pattern.
     * Matches a string that contains lowercase characters, digits, hyphens, underscores or dots.
     * The string may start and end only in characters or digits.
     * Note that '.' is allowed but not documented.
     */
    private static final Pattern VALID_ID_CHAR_PATTERN = Pattern.compile("[a-z0-9](?:[a-z0-9_\\-\\.]*[a-z0-9])?");

    public static final int ID_LENGTH_LIMIT = 64;

    private TransformStrings() {
    }

    public static boolean isValidId(String id) {
        return id != null && VALID_ID_CHAR_PATTERN.matcher(id).matches() && !Metadata.ALL.equals(id);
    }

    /**
     * Checks if the given {@code id} has a valid length.
     * We keep IDs in a length shorter or equal than {@link #ID_LENGTH_LIMIT}
     * in order to avoid unfriendly errors when storing docs with
     * more than 512 bytes.
     *
     * @param id the id
     * @return {@code true} if the id has a valid length
     */
    public static boolean hasValidLengthForId(String id) {
        return id.length() <= ID_LENGTH_LIMIT;
    }

    /**
     * Does the following checks:
     *
     *  - determines if there are any full duplicate names between the names
     *  - finds if there are conflicting name paths that could cause a failure later when the config is started.
     *
     * Examples showing conflicting field name paths:
     *
     * aggName1: foo.bar.baz
     * aggName2: foo.bar
     *
     * This should fail as aggName1 will cause foo.bar to be an object, causing a conflict with the use of foo.bar in aggName2.
     * @param usedNames The field names to check names
     * @return List of validation failure messages
     */
    public static List<String> detectDuplicateDotDelimitedPaths(List<String> usedNames) {
        if (usedNames == null || usedNames.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> validationFailures = new ArrayList<>();

        usedNames.sort(String::compareTo);
        for (int i = 0; i < usedNames.size() - 1; i++) {
            if (usedNames.get(i + 1).startsWith(usedNames.get(i) + ".")) {
                validationFailures.add("field [" + usedNames.get(i) + "] cannot be both an object and a field");
            }
            if (usedNames.get(i + 1).equals(usedNames.get(i))) {
                validationFailures.add("duplicate field [" + usedNames.get(i) + "] detected");
            }
        }

        for (String name : usedNames) {
            if (name.startsWith(".")) {
                validationFailures.add("field [" + name + "] must not start with '.'");
            }
            if (name.endsWith(".")) {
                validationFailures.add("field [" + name + "] must not end with '.'");
            }
        }

        return validationFailures;
    }
}
