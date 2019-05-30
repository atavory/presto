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

import com.facebook.presto.operator.aggregation.fixedhistogram.FixedDoubleBreakdownHistogram;
import com.google.common.collect.Streams;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.HashMap;
import java.util.Map;

public class SampleEntropyStateFixedHistogramJacknifeStrategy
        extends SampleEntropyStateFixedHistogramStrategy
{
    public SampleEntropyStateFixedHistogramJacknifeStrategy(long bucketCount, double min, double max)
    {
        super(new FixedDoubleBreakdownHistogram((int) bucketCount, min, max));
    }

    protected SampleEntropyStateFixedHistogramJacknifeStrategy(SampleEntropyStateFixedHistogramJacknifeStrategy other)
    {
        super(new FixedDoubleBreakdownHistogram(other.getBreakdownHistogram()));
    }

    public SampleEntropyStateFixedHistogramJacknifeStrategy(SliceInput input)
    {
        super(new FixedDoubleBreakdownHistogram(input));
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
    public void mergeWith(SampleEntropyStateStrategy other)
    {
        getBreakdownHistogram()
                .mergeWith(((SampleEntropyStateFixedHistogramJacknifeStrategy) other).getBreakdownHistogram());
    }

    @Override
    public void add(double value, double weight)
    {
        getBreakdownHistogram().add(value, weight);
    }

    @Override
    public double calculateEntropy()
    {
        final Map<Double, Double> bucketWeights = new HashMap<Double, Double>();
        Streams.stream(getBreakdownHistogram().iterator()).forEach(
                e -> {
                    bucketWeights.put(
                            e.left,
                            e.breakdown.entrySet().stream().mapToDouble(w -> w.getKey() * w.getValue()).sum());
                });
        final long n = Streams.stream(getBreakdownHistogram())
                .mapToLong(e -> e.breakdown.values().stream().mapToLong(i -> i).sum())
                .sum();
        final double sumW =
                bucketWeights.values().stream().mapToDouble(w -> w).sum();
        final double sumWLogW =
                bucketWeights.values().stream().mapToDouble(w -> w == 0.0 ? 0.0 : w * Math.log(w)).sum();

        double entropy = n * calculateEntropy(sumW, sumWLogW);
        entropy -= Streams.stream(getBreakdownHistogram().iterator()).mapToDouble(
                e -> {
                    final double bucketWeight = bucketWeights.get(e.left);
                    return e.breakdown.entrySet().stream().mapToDouble(
                            bucketE -> {
                                final double holdoutBucketWeight = Math.max(bucketWeight - bucketE.getKey(), 0);
                                final double holdoutSumW =
                                        sumW - bucketWeight + holdoutBucketWeight;
                                final double holdoutSumWLogW =
                                        sumWLogW - bucketWeight * Math.log(bucketWeight) +
                                                holdoutBucketWeight * Math.log(holdoutBucketWeight);
                                return bucketE.getValue() * (n - 1) *
                                        calculateEntropy(holdoutSumW, holdoutSumWLogW) /
                                        n;
                            })
                            .sum();
                })
                .sum();
        return entropy;
    }

    private double calculateEntropy(double sumW, double sumWLogW)
    {
        if (sumW == 0.0) {
            return 0.0;
        }
        final double entropy = Math.max(
                (Math.log(sumW) - sumWLogW / sumW) / Math.log(2.0),
                0.0);
        return entropy;
    }

    @Override
    public long estimatedInMemorySize()
    {
        return getBreakdownHistogram().estimatedInMemorySize();
    }

    @Override
    public int getRequiredBytesForSerialization()
    {
        return getBreakdownHistogram().getRequiredBytesForSerialization();
    }

    @Override
    public void serialize(SliceOutput out)
    {
        getBreakdownHistogram().serialize(out);
    }

    public FixedDoubleBreakdownHistogram getBreakdownHistogram()
    {
        return ((FixedDoubleBreakdownHistogram) super.histogram);
    }

    @Override
    public SampleEntropyStateStrategy clone()
    {
        return new SampleEntropyStateFixedHistogramJacknifeStrategy(this);
    }
}
