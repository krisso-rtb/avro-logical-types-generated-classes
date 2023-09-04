package com.rtbhouse.generated.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.Test;

public class SerializersTest {

    @Test
    void shouldSerialize() throws IOException {
        long now = System.currentTimeMillis();
        LocalTimestampRecord localTimestampRecord = LocalTimestampRecord.newBuilder()
                .setNestedTimestamp(now)
                .setUnionOfDateAndLocalTimestamp(now)
                .setNullableNestedTimestamp(now)
                .setNullableUnionOfDateAndLocalTimestamp(LocalDate.now())
                .build();

        FastSerdeLogicalTypesTest1 data = FastSerdeLogicalTypesTest1.newBuilder()
                .setNestedLocalTimestampMillis(localTimestampRecord)
                .setArrayOfDates(List.of(LocalDate.now()))
                .setDateField(LocalDate.now())
                .setNullableArrayOfDates(null)
                .setTimeMicrosField(LocalTime.now())
                .setTimeMillisField(LocalTime.now())
                .setUuidField(UUID.randomUUID().toString())
                .setTimestampMicrosField(Instant.now())
                .setTimestampMillisField(Instant.now())
                .setTimestampMillisMap(Map.of("now", Instant.now()))
                .setUnionOfArrayAndMap(Map.of())
                .setUnionOfDecimalOrDate(new BigDecimal("3.14"))
                .build();

        Schema schema = data.getSchema();
        SpecificData specificData = data.getSpecificData();

        List<Conversion<?>> missingConversions = List.of(
                // remove any item from below 3 and the test will fail due to exception
                new Conversions.DecimalConversion(),
                new TimeConversions.TimeMicrosConversion(),
                new TimeConversions.TimestampMicrosConversion()
        );

        for (Conversion<?> conversion : missingConversions) {
            specificData.addLogicalTypeConversion(conversion);
        }

        GenericData genericData = new GenericData();
        specificData.getConversions().forEach(genericData::addLogicalTypeConversion);

        DatumWriter<FastSerdeLogicalTypesTest1> writer = new GenericDatumWriter<>(schema, genericData);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(data, binaryEncoder);
        binaryEncoder.flush();
    }
}
