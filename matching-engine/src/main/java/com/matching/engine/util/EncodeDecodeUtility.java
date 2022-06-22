package com.matching.engine.util;

import com.ashish.marketdata.avro.Order;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class EncodeDecodeUtility {
    private static final Logger LOGGER = LoggerFactory.getLogger(EncodeDecodeUtility.class);

    public static byte[] serealizeAvroHttpRequestJSON(SpecificRecordBase request) {
        DatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>(
                (Class<SpecificRecordBase>) request.getClass());
        byte[] data = new byte[0];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder;
        try {
            jsonEncoder = EncoderFactory.get().jsonEncoder(
                    request.getSchema(), stream);
            writer.write(request, jsonEncoder);
            jsonEncoder.flush();
            data = stream.toByteArray();
        } catch (IOException e) {
            LOGGER.error("Serialization error:" + e.getMessage());
        }
        return data;
    }

    public SpecificRecordBase deSerealizeAvroHttpRequestJSON(byte[] data, ExSimCache.TXNTYPE txntype) {
        DatumReader<Order> reader
                = new SpecificDatumReader<>(Order.class);
        Decoder decoder;
        try {
            decoder = DecoderFactory.get().jsonDecoder(Order.getClassSchema(), new String(data));
            return reader.read(null, decoder);
        } catch (IOException e) {
            LOGGER.error("Deserialization error:" + e.getMessage());
        }
        return null;
    }


}
