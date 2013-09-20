/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may
 *     be used to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.util;

import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves object instances registered using the JDK 6+ {@link java.util.ServiceLoader}.
 *
 * @author aaraujo
 * @since 2.0
 */
public class InstanceResolver {
    private static final ConcurrentHashMap<Class, Object> RESOLVED_SINGLETONS = new ConcurrentHashMap<Class, Object>();

    private InstanceResolver() {/* not allowed */}

    /**
     * Resolves an instance of the specified class if it has not already been resolved.
     * @param clazz The type of instance to resolve
     * @param defaultInstance The instance to use if a custom instance has not been registered
     * @return The resolved instance or the default instance provided.
     *         {@code null} if an instance is not registered and a default is not provided.
     */
    @SuppressWarnings("unchecked")
    public static <T> T getSingleton(Class<T> clazz, T defaultInstance) {
        if (!RESOLVED_SINGLETONS.containsKey(clazz)) {
            // check the type of the default instance if provided
            if (defaultInstance != null && !clazz.isInstance(defaultInstance)) throw new IllegalArgumentException("defaultInstance is not of type " + clazz.getName());
            RESOLVED_SINGLETONS.put(clazz, resolveSingleton(clazz, defaultInstance));
        }
        return (T) RESOLVED_SINGLETONS.get(clazz);
    }

    private synchronized static <T> T resolveSingleton(Class<T> clazz, T defaultInstance) {
        ServiceLoader<T> loader = ServiceLoader.load(clazz);
        // returns the first registered instance found
        for (T singleton : loader) {
            return singleton;
        }
        return defaultInstance;
    }
}
