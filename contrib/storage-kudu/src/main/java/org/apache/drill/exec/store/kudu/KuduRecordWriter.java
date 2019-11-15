
package org.apache.drill.exec.store.kudu;

import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.fn.JsonOutput;
import java.io.IOException;
import java.lang.UnsupportedOperationException;
import java.util.List;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.io.api.Binary;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.common.types.TypeProtos;
import org.joda.time.DateTimeUtils;
import java.io.IOException;
import java.lang.UnsupportedOperationException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.io.api.Binary;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.common.types.TypeProtos;
import org.joda.time.DateTimeUtils;
import java.io.IOException;
import java.lang.UnsupportedOperationException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kudu.client.*;
import org.apache.drill.exec.store.*;

public abstract class KuduRecordWriter extends AbstractRecordWriter implements RecordWriter {

    private PartialRow row;

    public void setUp(PartialRow row) {
      this.row = row;
    }


























          @Override
          public FieldConverter getNewNullableIntConverter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableIntKuduConverter(fieldId, fieldName, reader);
          }

          public class NullableIntKuduConverter extends FieldConverter {
            private NullableIntHolder holder = new NullableIntHolder();

            public NullableIntKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              row.addInt(fieldId, holder.value);
            }
          }

          @Override
          public FieldConverter getNewIntConverter(int fieldId, String fieldName, FieldReader reader) {
            return new IntKuduConverter(fieldId, fieldName, reader);
          }

          public class IntKuduConverter extends FieldConverter {
            private NullableIntHolder holder = new NullableIntHolder();

            public IntKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              row.addInt(fieldId, holder.value);
            }
          }









          @Override
          public FieldConverter getNewNullableFloat4Converter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableFloat4KuduConverter(fieldId, fieldName, reader);
          }

          public class NullableFloat4KuduConverter extends FieldConverter {
            private NullableFloat4Holder holder = new NullableFloat4Holder();

            public NullableFloat4KuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              row.addFloat(fieldId, holder.value);
            }
          }

          @Override
          public FieldConverter getNewFloat4Converter(int fieldId, String fieldName, FieldReader reader) {
            return new Float4KuduConverter(fieldId, fieldName, reader);
          }

          public class Float4KuduConverter extends FieldConverter {
            private NullableFloat4Holder holder = new NullableFloat4Holder();

            public Float4KuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              row.addFloat(fieldId, holder.value);
            }
          }





















          @Override
          public FieldConverter getNewNullableBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableBigIntKuduConverter(fieldId, fieldName, reader);
          }

          public class NullableBigIntKuduConverter extends FieldConverter {
            private NullableBigIntHolder holder = new NullableBigIntHolder();

            public NullableBigIntKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              row.addLong(fieldId, holder.value);
            }
          }

          @Override
          public FieldConverter getNewBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
            return new BigIntKuduConverter(fieldId, fieldName, reader);
          }

          public class BigIntKuduConverter extends FieldConverter {
            private NullableBigIntHolder holder = new NullableBigIntHolder();

            public BigIntKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              row.addLong(fieldId, holder.value);
            }
          }



          @Override
          public FieldConverter getNewNullableUInt8Converter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableUInt8KuduConverter(fieldId, fieldName, reader);
          }

          public class NullableUInt8KuduConverter extends FieldConverter {
            private NullableUInt8Holder holder = new NullableUInt8Holder();

            public NullableUInt8KuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              throw new UnsupportedOperationException();
            }
          }

          @Override
          public FieldConverter getNewUInt8Converter(int fieldId, String fieldName, FieldReader reader) {
            return new UInt8KuduConverter(fieldId, fieldName, reader);
          }

          public class UInt8KuduConverter extends FieldConverter {
            private NullableUInt8Holder holder = new NullableUInt8Holder();

            public UInt8KuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              throw new UnsupportedOperationException();
            }
          }



          @Override
          public FieldConverter getNewNullableFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableFloat8KuduConverter(fieldId, fieldName, reader);
          }

          public class NullableFloat8KuduConverter extends FieldConverter {
            private NullableFloat8Holder holder = new NullableFloat8Holder();

            public NullableFloat8KuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              row.addDouble(fieldId, holder.value);
            }
          }

          @Override
          public FieldConverter getNewFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
            return new Float8KuduConverter(fieldId, fieldName, reader);
          }

          public class Float8KuduConverter extends FieldConverter {
            private NullableFloat8Holder holder = new NullableFloat8Holder();

            public Float8KuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              row.addDouble(fieldId, holder.value);
            }
          }









          @Override
          public FieldConverter getNewNullableTimeStampConverter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableTimeStampKuduConverter(fieldId, fieldName, reader);
          }

          public class NullableTimeStampKuduConverter extends FieldConverter {
            private NullableTimeStampHolder holder = new NullableTimeStampHolder();

            public NullableTimeStampKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              row.addLong(fieldId, holder.value*1000);
            }
          }

          @Override
          public FieldConverter getNewTimeStampConverter(int fieldId, String fieldName, FieldReader reader) {
            return new TimeStampKuduConverter(fieldId, fieldName, reader);
          }

          public class TimeStampKuduConverter extends FieldConverter {
            private NullableTimeStampHolder holder = new NullableTimeStampHolder();

            public TimeStampKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              row.addLong(fieldId, holder.value*1000);
            }
          }





















          @Override
          public FieldConverter getNewNullableDecimal28DenseConverter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableDecimal28DenseKuduConverter(fieldId, fieldName, reader);
          }

          public class NullableDecimal28DenseKuduConverter extends FieldConverter {
            private NullableDecimal28DenseHolder holder = new NullableDecimal28DenseHolder();

            public NullableDecimal28DenseKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              throw new UnsupportedOperationException();
            }
          }

          @Override
          public FieldConverter getNewDecimal28DenseConverter(int fieldId, String fieldName, FieldReader reader) {
            return new Decimal28DenseKuduConverter(fieldId, fieldName, reader);
          }

          public class Decimal28DenseKuduConverter extends FieldConverter {
            private NullableDecimal28DenseHolder holder = new NullableDecimal28DenseHolder();

            public Decimal28DenseKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              throw new UnsupportedOperationException();
            }
          }



          @Override
          public FieldConverter getNewNullableDecimal38DenseConverter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableDecimal38DenseKuduConverter(fieldId, fieldName, reader);
          }

          public class NullableDecimal38DenseKuduConverter extends FieldConverter {
            private NullableDecimal38DenseHolder holder = new NullableDecimal38DenseHolder();

            public NullableDecimal38DenseKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              throw new UnsupportedOperationException();
            }
          }

          @Override
          public FieldConverter getNewDecimal38DenseConverter(int fieldId, String fieldName, FieldReader reader) {
            return new Decimal38DenseKuduConverter(fieldId, fieldName, reader);
          }

          public class Decimal38DenseKuduConverter extends FieldConverter {
            private NullableDecimal38DenseHolder holder = new NullableDecimal38DenseHolder();

            public Decimal38DenseKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              throw new UnsupportedOperationException();
            }
          }















          @Override
          public FieldConverter getNewNullableVarBinaryConverter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableVarBinaryKuduConverter(fieldId, fieldName, reader);
          }

          public class NullableVarBinaryKuduConverter extends FieldConverter {
            private NullableVarBinaryHolder holder = new NullableVarBinaryHolder();

            public NullableVarBinaryKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              byte[] bytes = new byte[holder.end - holder.start];
              holder.buffer.getBytes(holder.start, bytes);
              row.addBinary(fieldId, bytes);
              reader.read(holder);
            }
          }

          @Override
          public FieldConverter getNewVarBinaryConverter(int fieldId, String fieldName, FieldReader reader) {
            return new VarBinaryKuduConverter(fieldId, fieldName, reader);
          }

          public class VarBinaryKuduConverter extends FieldConverter {
            private NullableVarBinaryHolder holder = new NullableVarBinaryHolder();

            public VarBinaryKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              byte[] bytes = new byte[holder.end - holder.start];
              holder.buffer.getBytes(holder.start, bytes);
              row.addBinary(fieldId, bytes);
              reader.read(holder);
            }
          }



          @Override
          public FieldConverter getNewNullableVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableVarCharKuduConverter(fieldId, fieldName, reader);
          }

          public class NullableVarCharKuduConverter extends FieldConverter {
            private NullableVarCharHolder holder = new NullableVarCharHolder();

            public NullableVarCharKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              byte[] bytes = new byte[holder.end - holder.start];
              holder.buffer.getBytes(holder.start, bytes);
              row.addString(fieldId, new String(bytes));
            }
          }

          @Override
          public FieldConverter getNewVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
            return new VarCharKuduConverter(fieldId, fieldName, reader);
          }

          public class VarCharKuduConverter extends FieldConverter {
            private NullableVarCharHolder holder = new NullableVarCharHolder();

            public VarCharKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              byte[] bytes = new byte[holder.end - holder.start];
              holder.buffer.getBytes(holder.start, bytes);
              row.addString(fieldId, new String(bytes));
            }
          }



          @Override
          public FieldConverter getNewNullableVar16CharConverter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableVar16CharKuduConverter(fieldId, fieldName, reader);
          }

          public class NullableVar16CharKuduConverter extends FieldConverter {
            private NullableVar16CharHolder holder = new NullableVar16CharHolder();

            public NullableVar16CharKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              throw new UnsupportedOperationException();
            }
          }

          @Override
          public FieldConverter getNewVar16CharConverter(int fieldId, String fieldName, FieldReader reader) {
            return new Var16CharKuduConverter(fieldId, fieldName, reader);
          }

          public class Var16CharKuduConverter extends FieldConverter {
            private NullableVar16CharHolder holder = new NullableVar16CharHolder();

            public Var16CharKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              throw new UnsupportedOperationException();
            }
          }



          @Override
          public FieldConverter getNewNullableVarDecimalConverter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableVarDecimalKuduConverter(fieldId, fieldName, reader);
          }

          public class NullableVarDecimalKuduConverter extends FieldConverter {
            private NullableVarDecimalHolder holder = new NullableVarDecimalHolder();

            public NullableVarDecimalKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              throw new UnsupportedOperationException();
            }
          }

          @Override
          public FieldConverter getNewVarDecimalConverter(int fieldId, String fieldName, FieldReader reader) {
            return new VarDecimalKuduConverter(fieldId, fieldName, reader);
          }

          public class VarDecimalKuduConverter extends FieldConverter {
            private NullableVarDecimalHolder holder = new NullableVarDecimalHolder();

            public VarDecimalKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              throw new UnsupportedOperationException();
            }
          }



          @Override
          public FieldConverter getNewNullableBitConverter(int fieldId, String fieldName, FieldReader reader) {
            return new NullableBitKuduConverter(fieldId, fieldName, reader);
          }

          public class NullableBitKuduConverter extends FieldConverter {
            private NullableBitHolder holder = new NullableBitHolder();

            public NullableBitKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {

            if (!reader.isSet()) {
              return;
            }

            reader.read(holder);

              row.addBoolean(fieldId, holder.value == 1);
            }
          }

          @Override
          public FieldConverter getNewBitConverter(int fieldId, String fieldName, FieldReader reader) {
            return new BitKuduConverter(fieldId, fieldName, reader);
          }

          public class BitKuduConverter extends FieldConverter {
            private NullableBitHolder holder = new NullableBitHolder();

            public BitKuduConverter(int fieldId, String fieldName, FieldReader reader) {
              super(fieldId, fieldName, reader);
            }

            @Override
            public void writeField() throws IOException {


            reader.read(holder);

              row.addBoolean(fieldId, holder.value == 1);
            }
          }


  }
