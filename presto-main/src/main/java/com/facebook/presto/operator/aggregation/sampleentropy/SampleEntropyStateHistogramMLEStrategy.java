/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.aggregation.sampleentropy;

import com.facebook.presto.operator.aggregation.DoubleHistogramAggregation;
import com.facebook.presto.operator.aggregation.NumericHistogram;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.Streams;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Math.toIntExact;

public class SampleEntropyStateHistogramMLEStrategy
        extends SampleEntropyStateStrategy
{
    protected final long bucketCount;
    protected final NumericHistogram histogram;

    public SampleEntropyStateHistogramMLEStrategy(long bucketCount)
    {
        this.bucketCount = bucketCount;
        this.histogram = new NumericHistogram(
                toIntExact(bucketCount),
                DoubleHistogramAggregation.ENTRY_BUFFER_SIZE);
    }

    protected SampleEntropyStateHistogramMLEStrategy(SampleEntropyStateHistogramMLEStrategy other)
    {
        this.bucketCount = other.bucketCount;
        this.histogram = other.histogram.clone();
    }

    public SampleEntropyStateHistogramMLEStrategy(SliceInput input)
    {
        this.bucketCount = input.readLong();
        this.histogram = new NumericHistogram(
                input,
                DoubleHistogramAggregation.ENTRY_BUFFER_SIZE);
    }

    @Override
    public void validateParams(
            long bucketCount,
            double sample,
            Double weight,
            Double arg0,
            Double arg1)
    {
        super.validateParams(bucketCount, sample, weight, arg0, arg1);

        if (this.bucketCount != bucketCount) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Inconsistent bucket count");
        }
        if (arg0 != null || arg1 != null) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Cannot specify more arguments to this entropy method");
        }
    }

    @Override
    public void add(double sample, double weight)
    {
        this.histogram.add(sample, weight);
    }

    @Override
    public double calculateEntropy()
    {
        final double sum = Streams.stream(histogram.iterator())
                .mapToDouble(w -> w.weight)
                .sum();
        if (sum == 0) {
            return 0.0;
        }

        return Streams.stream(histogram.iterator())
                .mapToDouble(e -> e.weight == 0.0 ? 0.0 : e.weight / sum * Math.log(sum / e.weight))
                .sum() / Math.log(2);
    }

    @Override
    public long estimatedInMemorySize()
    {
        return histogram.estimatedInMemorySize();
    }

    @Override
    public int getRequiredBytesForSerialization()
    {
        return SizeOf.SIZE_OF_LONG + // bucketCount
            histogram.getRequiredBytesForSerialization();
    }

    @Override
    public void serialize(SliceOutput out)
    {
        out.writeLong(bucketCount);
        histogram.serialize(out);
    }

    @Override
    public void mergeWith(SampleEntropyStateStrategy other)
    {
        histogram.mergeWith(((SampleEntropyStateHistogramMLEStrategy) other).histogram.clone());
    }

    @Override
    public SampleEntropyStateStrategy clone()
    {
        return new SampleEntropyStateHistogramMLEStrategy(this);
    }
}
