/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.descriptors;

import org.apache.flink.table.api.ValidationException;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link Msgpack} descriptor.
 */
public class MsgpackTest extends DescriptorTestBase {

	@Test(expected = ValidationException.class)
	public void testInvalidMissingField() {
		addPropertyAndVerify(descriptors().get(0), "format.fail-on-missing-field", "DDD");
	}

	@Test(expected = ValidationException.class)
	public void testDuplicateSchema() {
		// we add an additional non-json schema
		addPropertyAndVerify(descriptors().get(0), "format.schema", "DDD");
	}

	@Test(expected = ValidationException.class)
	public void testInvalidIgnoreParseErrors() {
		addPropertyAndVerify(descriptors().get(0), "format.ignore-parse-errors", "DDD");
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public List<Descriptor> descriptors() {
		final Descriptor desc1 = new Msgpack().failOnMissingField(false);

		final Descriptor desc2 = new Msgpack().ignoreParseErrors(true);

		return Arrays.asList(desc1, desc2);
	}

	@Override
	public List<Map<String, String>> properties() {
		final Map<String, String> props1 = new HashMap<>();
		props1.put("format.type", "msgpack");
		props1.put("format.property-version", "1");
		props1.put("format.fail-on-missing-field", "false");

		final Map<String, String> props2 = new HashMap<>();
		props2.put("format.type", "msgpack");
		props2.put("format.property-version", "1");
		props2.put("format.ignore-parse-errors", "true");

		return Arrays.asList(props1, props2);
	}

	@Override
	public DescriptorValidator validator() {
		return new MsgpackValidator();
	}
}
