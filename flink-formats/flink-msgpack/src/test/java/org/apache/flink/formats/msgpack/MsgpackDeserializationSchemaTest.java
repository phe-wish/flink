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

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MsgpackDeserializationSchema}.
 */
public class MsgpackDeserializationSchemaTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testSerDe() throws Exception {
		int intValue = 45536;
		float floatValue = 33.333F;
		long bigint = 1238123899121L;
		String name = "asdlkjasjkdla998y1122";

		MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
		packer.packMapHeader(5)
		    .packString("bool").packBoolean(true)
		    .packString("int").packInt(intValue)
			.packString("bigint").packLong(bigint)
			.packString("float").packFloat(floatValue)
			.packString("name").packString(name);

		DataType dataType = ROW(
			FIELD("bool", BOOLEAN()),
			FIELD("int", INT()),
			FIELD("bigint", BIGINT()),
			FIELD("float", FLOAT()),
			FIELD("name", STRING()));
		RowType schema = (RowType) dataType.getLogicalType();
		RowDataTypeInfo resultTypeInfo = new RowDataTypeInfo(schema);

		MsgpackDeserializationSchema deserializationSchema = new MsgpackDeserializationSchema(
			schema, resultTypeInfo, false, false);

		Row expected = new Row(5);
		expected.setField(0, true);
		expected.setField(1, intValue);
		expected.setField(2, bigint);
		expected.setField(3, floatValue);
		expected.setField(4, name);

		RowData rowData = deserializationSchema.deserialize(packer.toByteArray());
		Row actual = convertToExternal(rowData, dataType);
		assertEquals(expected, actual);
	}

	@SuppressWarnings("unchecked")
	private static Row convertToExternal(RowData rowData, DataType dataType) {
		return (Row) DataFormatConverters.getConverterForDataType(dataType).toExternal(rowData);
	}
}
