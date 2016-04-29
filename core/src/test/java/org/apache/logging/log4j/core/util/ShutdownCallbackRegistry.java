/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.logging.log4j.core.util;

/**
 * Dummy interface included because Druid's Initialization.makeInjectorWithModules has a runtime dependency on
 * log4j-core, but we don't want to include that as we are using logback.
 *
 * Only needed in test, since at runtime we use a stripped-down DruidGuicer rather than a full-on Druid injector.
 */
public interface ShutdownCallbackRegistry
{
}
