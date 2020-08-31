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

import java.util.Map;

import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA;
import static org.apache.flink.table.descriptors.MsgpackValidator.FORMAT_FAIL_ON_MISSING_FIELD;
import static org.apache.flink.table.descriptors.MsgpackValidator.FORMAT_IGNORE_PARSE_ERRORS;
import static org.apache.flink.table.descriptors.MsgpackValidator.FORMAT_MSGPACK_SCHEMA;
import static org.apache.flink.table.descriptors.MsgpackValidator.FORMAT_SCHEMA;
import static org.apache.flink.table.descriptors.MsgpackValidator.FORMAT_TYPE_VALUE;

/**
  * Format descriptor for Msgpack.
  */
public class Msgpack extends FormatDescriptor {

	private Boolean failOnMissingField;
	private Boolean deriveSchema;
	private Boolean ignoreParseErrors;
	private String msgpackSchema;
	private String schema;

	/**
	  * Format descriptor for Msgpack.
	  */
	public Msgpack() {
		super(FORMAT_TYPE_VALUE, 1);
	}

	/**
	 * Sets flag whether to fail if a field is missing or not.
	 *
	 * @param failOnMissingField If set to true, the operation fails if there is a missing field.
	 *                           If set to false, a missing field is set to null.
	 */
	public Msgpack failOnMissingField(boolean failOnMissingField) {
		this.failOnMissingField = failOnMissingField;
		return this;
	}

	/**
	 * Sets flag whether to fail when parsing msgpack fails.
	 *
	 * @param ignoreParseErrors If set to true, the operation will ignore parse errors.
	 *                          If set to false, the operation fails when parsing msgpack fails.
	 */
	public Msgpack ignoreParseErrors(boolean ignoreParseErrors) {
		this.ignoreParseErrors = ignoreParseErrors;
		return this;
	}

	@Override
	protected Map<String, String> toFormatProperties() {
		final DescriptorProperties properties = new DescriptorProperties();

		if (deriveSchema != null) {
			properties.putBoolean(FORMAT_DERIVE_SCHEMA, deriveSchema);
		}

		if (msgpackSchema != null) {
			properties.putString(FORMAT_MSGPACK_SCHEMA, msgpackSchema);
		}

		if (schema != null) {
			properties.putString(FORMAT_SCHEMA, schema);
		}

		if (failOnMissingField != null) {
			properties.putBoolean(FORMAT_FAIL_ON_MISSING_FIELD, failOnMissingField);
		}

		if (ignoreParseErrors != null) {
			properties.putBoolean(FORMAT_IGNORE_PARSE_ERRORS, ignoreParseErrors);
		}

		return properties.asMap();
	}
}
