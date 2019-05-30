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

import com.facebook.presto.operator.aggregation.fixedhistogram.FixedDoubleHistogram;
import com.google.common.collect.Streams;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public class SampleEntropyStateFixedHistogramMLEStrategy
        extends SampleEntropyStateFixedHistogramStrategy
{
    public SampleEntropyStateFixedHistogramMLEStrategy(long bucketCount, double min, double max)
    {
        super(new FixedDoubleHistogram((int) bucketCount, min, max));
    }

    protected SampleEntropyStateFixedHistogramMLEStrategy(SampleEntropyStateFixedHistogramMLEStrategy other)
    {
        super(other.getWeightHistogram().clone());
    }

    public SampleEntropyStateFixedHistogramMLEStrategy(SliceInput input)
    {
        super(new FixedDoubleHistogram(input));
    }

    @Override
    public void validateParams(
            long bucketCount,
            double sample,
            Double weight,
            Double min,
            Double max)
    {
        super.validateParams(bucketCount, sample, weight, min, max);
    }

    @Override
    public void add(double sample, double weight)
    {
        getWeightHistogram().add(sample, weight);
    }

    @Override
    public double calculateEntropy()
    {
        final double sum = Streams.stream(getWeightHistogram().iterator())
                .mapToDouble(w -> w.weight)
                .sum();
        if (sum == 0) {
            return 0.0;
        }

        return Streams.stream(getWeightHistogram().iterator())
                .mapToDouble(w -> {
                    final double width = w.right - w.left;
                    final double prob = w.weight / sum;
                    return prob == 0 ? 0 : prob * Math.log(width / prob);
                })
                .sum() / Math.log(2);
    }

    @Override
    public long estimatedInMemorySize()
    {
        return getWeightHistogram().estimatedInMemorySize();
    }

    @Override
    public int getRequiredBytesForSerialization()
    {
        return getWeightHistogram().getRequiredBytesForSerialization();
    }

    @Override
    public void mergeWith(SampleEntropyStateStrategy other)
    {
        getWeightHistogram()
                .mergeWith(((SampleEntropyStateFixedHistogramMLEStrategy) other).getWeightHistogram());
    }

    @Override
    public void serialize(SliceOutput out)
    {
        getWeightHistogram().serialize(out);
    }

    public FixedDoubleHistogram getWeightHistogram()
    {
        return ((FixedDoubleHistogram) super.histogram);
    }

    @Override
    public SampleEntropyStateStrategy clone()
    {
        return new SampleEntropyStateFixedHistogramMLEStrategy(this);
    }
}
