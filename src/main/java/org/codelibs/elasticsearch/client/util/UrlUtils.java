/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.elasticsearch.client.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.client.action.HttpAction;

public final class UrlUtils {

    private static Logger logger = LogManager.getLogger(HttpAction.class);

    private UrlUtils() {
        // nothing
    }

    public static String joinAndEncode(final CharSequence delimiter, final CharSequence... elements) {
        if (elements == null) {
            return null;
        }
        return Arrays.stream(elements).map(s -> UrlUtils.encode(s)).collect(Collectors.joining(delimiter));
    }

    public static String encode(final CharSequence element) {
        if (element == null) {
            return null;
        }
        try {
            return URLEncoder.encode(element.toString(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Invalid encoding.", e);
            }
            return element.toString();
        }
    }
}
