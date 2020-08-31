package org.apache.flink.formats.msgpack;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.apache.flink.util.Preconditions.checkNotNull;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.util.Collector;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePackException;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.ValueType;

public class MsgpackDeserializationSchema implements DeserializationSchema<RowData> {
	private static final long serialVersionUID = 1L;

	private final RowType rowType;
	/** Flag indicating whether to fail if a field is missing. */
	private final boolean failOnMissingField;

	/** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
	private final boolean ignoreParseErrors;

	/** TypeInformation of the produced {@link RowData}. **/
	private final TypeInformation<RowData> resultTypeInfo;

	public MsgpackDeserializationSchema(
		RowType rowType,
		TypeInformation<RowData> resultTypeInfo,
		boolean failOnMissingField,
		boolean ignoreParseErrors) {
		if (ignoreParseErrors && failOnMissingField) {
			throw new IllegalArgumentException(
				"Msgpack format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
		}
		this.rowType = checkNotNull(rowType);
		this.resultTypeInfo = checkNotNull(resultTypeInfo);
		this.failOnMissingField = failOnMissingField;
		this.ignoreParseErrors = ignoreParseErrors;
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		try {
			final DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
				.map(RowType.RowField::getType)
				.map(this::createConverter)
				.toArray(DeserializationRuntimeConverter[]::new);
			final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

			MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(message, 0, message.length);
			Map<String, ImmutableValue> ret = Maps.newHashMap();

			int rows = unpacker.unpackMapHeader();
			for (int i = 0; i < rows; ++i) {
				final String key = unpacker.unpackString();
				final ImmutableValue value = unpacker.unpackValue();
				ret.put(key, value);
			}
			int arity = fieldNames.length;
			GenericRowData row = new GenericRowData(arity);
			for (int i = 0; i < arity; i++) {
				String fieldName = fieldNames[i];
				ImmutableValue field = ret.get(fieldName);
				Object convertedField = convertField(fieldConverters[i], fieldName, field);
				row.setField(i, convertedField);
			}
			return row;
		} catch (Throwable t) {
			if (ignoreParseErrors) {
				return null;
			}
			throw new IOException(format("Failed to deserialize JSON '%s'.", new String(message)), t);
		}
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return resultTypeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		MsgpackDeserializationSchema that = (MsgpackDeserializationSchema) o;
		return failOnMissingField == that.failOnMissingField &&
			ignoreParseErrors == that.ignoreParseErrors &&
			resultTypeInfo.equals(that.resultTypeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(failOnMissingField, ignoreParseErrors, resultTypeInfo);
	}

	// -------------------------------------------------------------------------------------
	// Runtime Converters
	// -------------------------------------------------------------------------------------

	/**
	 * Runtime converter that converts {@link JsonNode}s into objects of Flink Table & SQL
	 * internal data structures.
	 */
	@FunctionalInterface
	private interface DeserializationRuntimeConverter extends Serializable {
		Object convert(ImmutableValue field);
	}

	/**
	 * Creates a runtime converter which is null safe.
	 */
	private DeserializationRuntimeConverter createConverter(LogicalType type) {
		return wrapIntoNullableConverter(createNotNullConverter(type));
	}

	/**
	 * Creates a runtime converter which assuming input object is not null.
	 */
	private DeserializationRuntimeConverter createNotNullConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return field -> null;
			case BOOLEAN:
				return this::convertToBoolean;
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return this::convertToInt;
			case BIGINT:
				return this::convertToLong;
			case FLOAT:
				return this::convertToFloat;
			case DOUBLE:
				return this::convertToDouble;
			case CHAR:
			case VARCHAR:
				return this::convertToString;
			case BINARY:
			case VARBINARY:
			case DECIMAL:
			case ARRAY:
			case INTERVAL_DAY_TIME:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case ROW:
			case TINYINT:
			case SMALLINT:
			case RAW:
			case MAP:
			case MULTISET:
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	private boolean convertToBoolean(ImmutableValue field) {
		if (field.getValueType() == ValueType.BOOLEAN) {
			// avoid redundant toString and parseBoolean, for better performance
			return field.asBooleanValue().getBoolean();
		} else {
			return Boolean.parseBoolean(field.asStringValue().asString().trim());
		}
	}

	private int convertToInt(ImmutableValue field) {
		if (field.getValueType() == ValueType.INTEGER) {
			// avoid redundant toString and parseInt, for better performance
			return field.asIntegerValue().toInt();
		} else {
			return Integer.parseInt(field.asStringValue().asString().trim());
		}
	}

	private long convertToLong(ImmutableValue field) {
		if (field.getValueType() == ValueType.INTEGER) {
			// avoid redundant toString and parseLong, for better performance
			return field.asIntegerValue().toLong();
		} else {
			return Long.parseLong(field.asStringValue().asString().trim());
		}
	}

	private double convertToDouble(ImmutableValue field) {
		if (field.getValueType() == ValueType.FLOAT) {
			// avoid redundant toString and parseDouble, for better performance
			return field.asFloatValue().toDouble();
		} else {
			return Double.parseDouble(field.asStringValue().asString().trim());
		}
	}

	private float convertToFloat(ImmutableValue field) {
		if (field.getValueType() == ValueType.FLOAT) {
			// avoid redundant toString and parseDouble, for better performance
			return field.asFloatValue().toFloat();
		} else {
			return Float.parseFloat(field.asStringValue().asString().trim());
		}
	}

	private StringData convertToString(ImmutableValue field) {
		return StringData.fromString(field.asStringValue().toString());
	}

	private Object convertField(
		DeserializationRuntimeConverter fieldConverter,
		String fieldName,
		ImmutableValue field) {
		if (field == null) {
			if (failOnMissingField) {
				throw new MsgpackParseException(
					"Could not find field with name '" + fieldName + "'.");
			} else {
				return null;
			}
		} else {
			return fieldConverter.convert(field);
		}
	}

	private DeserializationRuntimeConverter wrapIntoNullableConverter(
		DeserializationRuntimeConverter converter) {
		return field -> {
			if (field == null || field.isNilValue()) {
				return null;
			}
			try {
				return converter.convert(field);
			} catch (Throwable t) {
				if (!ignoreParseErrors) {
					throw t;
				}
				return null;
			}
		};
	}

	/**
	 * Exception which refers to parse errors in converters.
	 * */
	private static final class MsgpackParseException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public MsgpackParseException(String message) {
			super(message);
		}

		public MsgpackParseException(String message, Throwable cause) {
			super(message, cause);
		}
	}
}
