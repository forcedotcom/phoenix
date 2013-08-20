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
package com.salesforce.phoenix.query;

import com.salesforce.phoenix.util.InstanceResolver;

/**
 * Manages factories that provide extension points for HBase.
 * <p/>
 * Dependent modules may register their own implementations of the following using {@link java.util.ServiceLoader}:
 * <ul>
 *     <li>{@link ConfigurationFactory}</li>
 *     <li>{@link HTableFactory}</li>
 * </ul>
 *
 * If a custom implementation is not registered, the default implementations will be used.
 *
 * @author aaraujo
 * @since 0.2
 */
public class HBaseFactoryProvider {

    private static final HTableFactory DEFAULT_HTABLE_FACTORY = new HTableFactory.HTableFactoryImpl();
    private static final ConfigurationFactory DEFAULT_CONFIGURATION_FACTORY = new ConfigurationFactory.ConfigurationFactoryImpl();

    public static HTableFactory getHTableFactory() {
        return InstanceResolver.getSingleton(HTableFactory.class, DEFAULT_HTABLE_FACTORY);
    }

    public static ConfigurationFactory getConfigurationFactory() {
        return InstanceResolver.getSingleton(ConfigurationFactory.class, DEFAULT_CONFIGURATION_FACTORY);
    }
}
