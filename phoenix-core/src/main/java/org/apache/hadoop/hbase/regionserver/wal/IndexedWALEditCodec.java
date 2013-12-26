package org.apache.hadoop.hbase.regionserver.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.codec.BaseDecoder;
import org.apache.hadoop.hbase.codec.BaseEncoder;
import org.apache.hadoop.hbase.codec.Decoder;
import org.apache.hadoop.hbase.codec.Encoder;

import com.salesforce.hbase.index.wal.IndexedKeyValue;
import com.salesforce.hbase.index.wal.KeyValueCodec;


/**
 * Support custom indexing {@link KeyValue}s when written to the WAL.
 * <p>
 * Currently, we don't support reading older WAL files - only new WAL files. Therefore, this should
 * not be installed on a running cluster, but rather one that has been cleanly shutdown and requires
 * no WAL replay on startup.
 */
public class IndexedWALEditCodec extends WALEditCodec {

  // can't have negative values because reading off a stream returns a negative if its the end of
  // the stream
  private static final int REGULAR_KEY_VALUE_MARKER = 0;
  private CompressionContext compression;

  /** Required nullary constructor */
  public IndexedWALEditCodec() {
  }

  /**
   * Override the parent implementation so we can get access to the current context too
   * @param compression compression to support for the encoder/decoder
   */
  @Override
  public void setCompression(CompressionContext compression) {
    super.setCompression(compression);
    this.compression = compression;
  }

  @Override
  public Decoder getDecoder(InputStream is) {
    // compression isn't enabled
    if (this.compression == null) {
      return new IndexKeyValueDecoder(is);
    }

    // there is compression, so we get the standard decoder to handle reading those kvs
    Decoder decoder = super.getDecoder(is);
    // compression is on, reqturn our custom decoder
    return new CompressedIndexKeyValueDecoder(is, decoder);
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    // compression isn't on, do the default thing
    if (this.compression == null) {
      return new IndexKeyValueEncoder(os);
    }

    // compression is on, return our one that will handle putting in the correct markers
    Encoder encoder = super.getEncoder(os);
    return new CompressedIndexKeyValueEncoder(os, encoder);
  }

  /**
   * Custom {@link Decoder} that can handle a stream of regular and indexed {@link KeyValue}s.
   */
  public class IndexKeyValueDecoder extends BaseDecoder {

    /**
     * Create a {@link Decoder} on the given input stream with the given {@link Decoder} to parse
     * generic {@link KeyValue}s.
     * @param is stream to read from
     */
    public IndexKeyValueDecoder(InputStream is){
      super(is);
    }

    @Override
    protected KeyValue parseCell() throws IOException{
      return KeyValueCodec.readKeyValue((DataInput) this.in);
    }
  }

  public class CompressedIndexKeyValueDecoder extends BaseDecoder {

    private Decoder decoder;

    /**
     * Create a {@link Decoder} on the given input stream with the given {@link Decoder} to parse
     * generic {@link KeyValue}s.
     * @param is stream to read from
     * @param compressedDecoder decoder for generic {@link KeyValue}s. Should support the expected
     *          compression.
     */
    public CompressedIndexKeyValueDecoder(InputStream is, Decoder compressedDecoder) {
      super(is);
      this.decoder = compressedDecoder;
    }

    @Override
    protected KeyValue parseCell() throws IOException {
      // reader the marker
      int marker = this.in.read();
      if (marker < 0) {
        throw new EOFException(
            "Unexepcted end of stream found while reading next (Indexed) KeyValue");
      }

      // do the normal thing, if its a regular kv
      if (marker == REGULAR_KEY_VALUE_MARKER) {
        if (!this.decoder.advance()) {
          throw new IOException("Could not read next key-value from generic KeyValue Decoder!");
        }
        return this.decoder.current();
      }

      // its an indexedKeyValue, so parse it out specially
      return KeyValueCodec.readKeyValue((DataInput) this.in);
    }
  }

  /**
   * Encode {@link IndexedKeyValue}s via the {@link KeyValueCodec}. Does <b>not</b> support
   * compression.
   */
  private static class IndexKeyValueEncoder extends BaseEncoder {
    public IndexKeyValueEncoder(OutputStream os) {
      super(os);
    }

    @Override
    public void flush() throws IOException {
      super.flush();
    }

    @Override
    public void write(KeyValue cell) throws IOException {
      // make sure we are open
      checkFlushed();

      // use the standard encoding mechanism
      KeyValueCodec.write((DataOutput) this.out, cell);
    }
  }

  /**
   * Write {@link IndexedKeyValue}s along side compressed {@link KeyValue}s. This Encoder is
   * <b>not</b> compatible with the {@link IndexKeyValueDecoder} - one cannot intermingle compressed
   * and uncompressed WALs that contain index entries.
   */
  private static class CompressedIndexKeyValueEncoder extends BaseEncoder {
    private Encoder compressedKvEncoder;

    public CompressedIndexKeyValueEncoder(OutputStream os, Encoder compressedKvEncoder) {
      super(os);
      this.compressedKvEncoder = compressedKvEncoder;
    }

    @Override
    public void flush() throws IOException {
      this.compressedKvEncoder.flush();
      super.flush();
    }

    @Override
    public void write(KeyValue cell) throws IOException {
      //make sure we are open
      checkFlushed();
      
      //write the special marker so we can figure out which kind of kv is it
      int marker = IndexedWALEditCodec.REGULAR_KEY_VALUE_MARKER;
      if (cell instanceof IndexedKeyValue) {
        marker = KeyValueCodec.INDEX_TYPE_LENGTH_MARKER;
      }
      out.write(marker);
      
      //then serialize based on the marker
      if (marker == IndexedWALEditCodec.REGULAR_KEY_VALUE_MARKER) {
        this.compressedKvEncoder.write(cell);
      }
      else{
        KeyValueCodec.write((DataOutput) out, cell);
      }
    }
  }
}