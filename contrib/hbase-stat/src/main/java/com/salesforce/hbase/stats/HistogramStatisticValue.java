package com.salesforce.hbase.stats;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.hbase.protobuf.generated.StatisticProtos.Histogram;

/**
 * {@link StatisticValue} whose value is actually a histogram of data.
 * <p>
 * Two different types of histograms are supported - fixed width and fixed depth.
 * <ol>
 *  <li>Fixed Width - the width of the each column is <b>fixed</b>, but there is a variable number
 *  of elements in each 'bucket' in the histogram. This is the 'usual' histogram.
 *    <ul>
 *      <li>You should only use {@link #addColumn(byte[])} as the width of each column is 
 *      known</li>
 *    </ul>
 *  </li>
 *  <li>Fixed Depth - the width of the each column is <b>variable</b>, but there are a known number 
 *  of elements in each 'bucket' in the histogram. For instance, this can be used to determine
 *  every n'th key
 *    <ul>
 *      <li>You should only use {@link #addColumn(int, byte[])} as the depth of each column is
 *       variable</li>
 *    </ul>
 *  </li>
 * </ol>
 */
public class HistogramStatisticValue extends StatisticValue {

  private Histogram.Builder builder = Histogram.newBuilder();

  /**
   * Build a statistic value - should only be used by the
   * {@link com.salesforce.hbase.stats.serialization.HistogramStatisticReader}
   * .
   * @param value statistic instance to reference - no data is copied
   * @throws InvalidProtocolBufferException if the data in the {@link StatisticValue} is not a
   *           histogram
   */
  public HistogramStatisticValue(StatisticValue value) throws InvalidProtocolBufferException {
    super(value.name, value.info, value.value);
    // reset the builder based on the data
    builder = Histogram.parseFrom(value.value).toBuilder();
  }

  /**
   * Build a fixed-width/depth histogram.
   * @param name name of the statistic
   * @param info general info about the statistic
   * @param widthOrDepth width of all the columns
   */
  public HistogramStatisticValue(byte[] name, byte[] info, long widthOrDepth) {
    super(name, info, null);
    this.builder.setDepthOrWidth(widthOrDepth);
  }

  /**
   * Add a new fixed-depth column to this histogram - we already know the depth of the column (it
   * was specified in the constructor), so we just need to get the next key boundary.
   * @param value value of the next column - added to the end of the histogram
   */
  public void addColumn(ByteString value) {
    builder.addValue(value);
  }

  /**
   * Add a new fixed-width column to the histogram. We already know the width of the column, so we
   * only care about getting the count of the keys in that column
   * @param count count of the keys in the column
   */
  public void addColumn(long count) {
    builder.addValue(ByteString.copyFrom(Bytes.toBytes(count)));
  }

  /**
   * Get the raw bytes for the histogram. Can be rebuilt using {@link #getHistogram(byte[])}. This
   * is a single-use method - after calling any previous calls to {@link #addColumn} are ignored.
   */
  public byte[] getValue() {
    byte[] data = builder.build().toByteArray();
    builder = Histogram.newBuilder();
    return data;
  }

  /**
   * Get the underlying columns for the histogram. After calling this method, any futher updates to
   * the histogram are not guarranteed to work correctly. original columns</b> every time.
   * @return a deserialized version of the histogram
   */
  public synchronized Histogram getHistogram() {
    return this.builder.build();
  }

  public static Histogram getHistogram(byte[] raw) throws InvalidProtocolBufferException {
    return Histogram.parseFrom(raw);
  }
}