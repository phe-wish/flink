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

package org.apache.flink.formats.msgpack;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * Format factory for providing configured instances of Debezium JSON to RowData {@link DeserializationSchema}.
 */
public class MsgpackFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

	public static final String IDENTIFIER = "msgpack";

	public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = ConfigOptions
		.key("fail-on-missing-field")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag to specify whether to fail if a field is missing or not, false by default");

	public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions
		.key("ignore-parse-errors")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag to skip fields and rows with parse errors instead of failing;\n"
			+ "fields are set to null in case of errors, false by default");

	@SuppressWarnings("unchecked")
	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);
		final boolean failOnMissingField = formatOptions.get(FAIL_ON_MISSING_FIELD);
		final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);

		return new DecodingFormat<DeserializationSchema<RowData>>() {
			@Override
			public DeserializationSchema<RowData> createRuntimeDecoder(
					DynamicTableSource.Context context, DataType producedDataType) {
				final RowType rowType = (RowType) producedDataType.getLogicalType();
				final TypeInformation<RowData> rowDataTypeInfo =
					(TypeInformation<RowData>) context.createTypeInformation(producedDataType);
				return new MsgpackDeserializationSchema(
					rowType,
					rowDataTypeInfo,
					failOnMissingField,
					ignoreParseErrors);
			}

			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.newBuilder()
					.addContainedKind(RowKind.INSERT)
					.addContainedKind(RowKind.UPDATE_BEFORE)
					.addContainedKind(RowKind.UPDATE_AFTER)
					.addContainedKind(RowKind.DELETE)
					.build();
			}
		};
	}

	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {
		throw new UnsupportedOperationException("Msgpack format doesn't support as a sink format yet.");
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.emptySet();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(FAIL_ON_MISSING_FIELD);
		options.add(IGNORE_PARSE_ERRORS);
		return options;
	}

}
