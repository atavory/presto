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
import io.airlift.slice.Slices;

import java.util.Iterator;

public class FixedDoubleHistogram
        extends FixedHistogram
        implements Cloneable, Iterable<FixedDoubleHistogram.Bin>
{
    protected final double[] weights;

    public class Bin
    {
        public final Double left;
        public final Double right;
        public final Double weight;

        public Bin(Double left, Double right, Double weight)
        {
            this.left = left;
            this.right = right;
            this.weight = weight;
        }
    }

    public FixedDoubleHistogram(int bucketCount, double min, double max)
    {
        super(bucketCount, min, max);
        weights = new double[super.getBucketCount()];
    }

    public FixedDoubleHistogram(SliceInput input)
    {
        super(input);
        weights = new double[super.getBucketCount()];
        input.readBytes(
                Slices.wrappedDoubleArray(weights),
                super.getBucketCount() * SizeOf.SIZE_OF_DOUBLE);
    }

    protected FixedDoubleHistogram(FixedDoubleHistogram other)
    {
        this(other.getBucketCount(), other.getMin(), other.getMax());
        for (int i = 0; i < super.getBucketCount(); ++i) {
            weights[i] = other.weights[i];
        }
    }

    public int getRequiredBytesForSerialization()
    {
        return super.getRequiredBytesForSerialization() +
                SizeOf.SIZE_OF_DOUBLE * super.getBucketCount(); // weights
    }

    public void serialize(SliceOutput out)
    {
        super.serialize(out);
        out.appendBytes(Slices.wrappedDoubleArray(weights, 0, super.getBucketCount()));
    }

    public long estimatedInMemorySize()
    {
        return super.estimatedInMemorySize() + SizeOf.sizeOf(weights);
    }

    public void set(double value, double weight)
    {
        weights[super.getIndexForValue(value)] = weight;
    }

    public void add(double value)
    {
        add(value, 1.0);
    }

    public void add(double value, double weight)
    {
        weights[getIndexForValue(value)] += weight;
    }

    public void mergeWith(FixedDoubleHistogram other)
    {
        super.mergeWith(other);
        for (int i = 0; i < super.getBucketCount(); ++i) {
            weights[i] += other.weights[i];
        }
    }

    @Override
    public Iterator<Bin> iterator()
    {
        final int bucketCount = super.getBucketCount();
        final double min = super.getMin();
        final double max = super.getMax();

        Iterator<FixedHistogram.Bin> baseIterator = super.getIterator();

        return new Iterator<Bin>()
        {
            final Iterator<FixedHistogram.Bin> iterator = baseIterator;

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Bin next()
            {
                if (!hasNext()) {
                    throw new UnsupportedOperationException();
                }

                final FixedHistogram.Bin baseValue = iterator.next();

                return new Bin(baseValue.left, baseValue.right, weights[baseValue.index]);
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public FixedDoubleHistogram clone()
    {
        return new FixedDoubleHistogram(this);
    }
}
