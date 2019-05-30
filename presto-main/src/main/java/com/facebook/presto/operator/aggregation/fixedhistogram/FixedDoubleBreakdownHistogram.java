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
package com.facebook.presto.operator.aggregation.fixedhistogram;

import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class FixedDoubleBreakdownHistogram
        extends FixedHistogram
        implements Iterable<FixedDoubleBreakdownHistogram.Bucket>
{
    protected ArrayList<Map<Double, Long>> breakdowns;

    public static class Bucket
    {
        public final double left;
        public final double right;
        public final Map<Double, Long> breakdown;

        public Bucket(double left, double right, Map<Double, Long> breakdown)
        {
            this.left = left;
            this.right = right;
            this.breakdown = breakdown;
        }
    }

    public FixedDoubleBreakdownHistogram(int bucketCount, double min, double max)
    {
        super(bucketCount, min, max);
        breakdowns = new ArrayList<Map<Double, Long>>();
        for (int i = 0; i < super.getBucketCount(); ++i) {
            breakdowns.add(new HashMap<Double, Long>());
        }
    }

    public FixedDoubleBreakdownHistogram(SliceInput input)
    {
        super(input);
        breakdowns = new ArrayList<Map<Double, Long>>();
        for (int i = 0; i < super.getBucketCount(); ++i) {
            final Map<Double, Long> breakdown = new HashMap<Double, Long>();
            final long numInBucket = input.readLong();
            for (int j = 0; j < numInBucket; ++j) {
                final double weight = input.readDouble();
                final long multiplicity = input.readLong();
                breakdown.put(weight, multiplicity);
            }
            breakdowns.add(breakdown);
        }
    }

    public FixedDoubleBreakdownHistogram(FixedDoubleBreakdownHistogram other)
    {
        super(other.getBucketCount(), other.getMin(), other.getMax());
        breakdowns = new ArrayList<Map<Double, Long>>();
        for (int i = 0; i < super.getBucketCount(); ++i) {
            breakdowns.add(new HashMap<Double, Long>(other.breakdowns.get(i)));
        }
    }

    public int getRequiredBytesForSerialization()
    {
        int size = super.getRequiredBytesForSerialization();
        for (int i = 0; i < super.getBucketCount(); ++i) {
            size += SizeOf.SIZE_OF_LONG;
            size += breakdowns.get(i).entrySet().stream()
                    .mapToInt(e -> SizeOf.SIZE_OF_DOUBLE + SizeOf.SIZE_OF_LONG)
                    .sum();
        }
        return size;
    }

    public void serialize(SliceOutput out)
    {
        super.serialize(out);
        for (int i = 0; i < super.getBucketCount(); ++i) {
            out.appendLong(breakdowns.get(i).size());
            breakdowns.get(i).entrySet()
                    .forEach(e -> {
                        out.appendDouble(e.getKey());
                        out.appendLong(e.getValue());
                    });
        }
    }

    public long estimatedInMemorySize()
    {
        return super.estimatedInMemorySize() + breakdowns
                .stream()
                .mapToLong(b -> b.size() * (Long.SIZE + Double.SIZE))
                .sum();
    }

    public void add(double value)
    {
        add(value, 1.0);
    }

    public void add(double value, double weight)
    {
        final Map<Double, Long> breakdown = breakdowns.get(getIndexForValue(value));
        breakdown.put(
                weight,
                breakdown.getOrDefault(weight, Long.valueOf(0)) + Long.valueOf(1));
    }

    public void mergeWith(FixedDoubleBreakdownHistogram other)
    {
        super.mergeWith(other);
        for (int i = 0; i < super.getBucketCount(); ++i) {
            final Map<Double, Long> breakdown = breakdowns.get(i);
            other.breakdowns.get(i).entrySet().stream().forEach(
                    e -> breakdown.put(
                            e.getKey(),
                            breakdown.getOrDefault(e.getKey(), Long.valueOf(0)) + e.getValue()));
        }
    }

    @Override
    public Iterator<Bucket> iterator()
    {
        final int bucketCount = super.getBucketCount();
        final double min = super.getMin();
        final double max = super.getMax();

        Iterator<FixedHistogram.Bucket> baseIterator = super.getIterator();

        return new Iterator<Bucket>()
        {
            final Iterator<FixedHistogram.Bucket> iterator = baseIterator;

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Bucket next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                final FixedHistogram.Bucket baseValue = iterator.next();

                return new Bucket(baseValue.left, baseValue.right, breakdowns.get(baseValue.index));
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
