package orderlifecycle;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;

public class RowFieldExtractorSchema implements SerializationSchema<Row> {

    private final int fieldIndex;

    public RowFieldExtractorSchema(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }

    @Override
    public byte[] serialize(Row row) {
        Object value = row.getField(fieldIndex);

        if (value == null) {
            return null;
        }

        return value.toString().getBytes(StandardCharsets.UTF_8);
    }
}
