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

import com.facebook.presto.operator.aggregation.AbstractTestAggregationFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.wrappedBuffer;

public abstract class TestSampleEntropyAggregation
        extends AbstractTestAggregationFunction
{
    protected static final Integer SIZE = 12;
    private final String method;
    private final Double arg0;
    private final Double arg1;

    protected TestSampleEntropyAggregation(String method, Double arg0, Double arg1)
    {
        this.method = method;
        this.arg0 = arg0;
        this.arg1 = arg1;
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder bucketCountBlockBuilder = BIGINT.createBlockBuilder(null, 2 * length);
        BlockBuilder sampleBlockBuilder = DOUBLE.createBlockBuilder(null, 2 * length);
        BlockBuilder weightBlockBuilder = DOUBLE.createBlockBuilder(null, 2 * length);
        BlockBuilder methodBlockBuilder = VARCHAR.createBlockBuilder(null, 2 * length);
        BlockBuilder arg0BlockBuilder = null;
        BlockBuilder arg1BlockBuilder = null;
        if (this.arg1 != null) {
            arg0BlockBuilder = DOUBLE.createBlockBuilder(null, 2 * length);
            arg1BlockBuilder = DOUBLE.createBlockBuilder(null, 2 * length);
        }
        else if (this.arg0 != null) {
            arg0BlockBuilder = DOUBLE.createBlockBuilder(null, 2 * length);
        }
        for (int weight = 1; weight < 3; ++weight) {
            for (int i = start; i < start + length; i++) {
                BIGINT.writeLong(bucketCountBlockBuilder, TestSampleEntropyAggregation.SIZE);
                DOUBLE.writeDouble(
                        sampleBlockBuilder,
                        Double.valueOf(
                                Math.min(Math.max(i, 0), TestSampleEntropyAggregation.SIZE)));
                VARCHAR.writeSlice(methodBlockBuilder, wrappedBuffer(this.method.getBytes()));
                DOUBLE.writeDouble(weightBlockBuilder, Double.valueOf(weight));
                if (this.arg1 != null) {
                    DOUBLE.writeDouble(arg0BlockBuilder, this.arg0);
                    DOUBLE.writeDouble(arg1BlockBuilder, this.arg1);
                }
                else if (this.arg0 != null) {
                    DOUBLE.writeDouble(arg0BlockBuilder, this.arg0);
                }
            }
        }

        if (this.arg1 != null) {
            return new Block[]{
                    bucketCountBlockBuilder.build(),
                    sampleBlockBuilder.build(),
                    weightBlockBuilder.build(),
                    methodBlockBuilder.build(),
                    arg0BlockBuilder.build(),
                    arg1BlockBuilder.build()
            };
        }
        if (this.arg0 != null) {
            return new Block[]{
                    bucketCountBlockBuilder.build(),
                    sampleBlockBuilder.build(),
                    weightBlockBuilder.build(),
                    methodBlockBuilder.build(),
                    arg0BlockBuilder.build()
            };
        }
        return new Block[]{
                bucketCountBlockBuilder.build(),
                sampleBlockBuilder.build(),
                weightBlockBuilder.build(),
                methodBlockBuilder.build()
        };
    }

    @Override
    protected String getFunctionName()
    {
        return "sample_entropy";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        if (this.arg1 != null) {
            return ImmutableList.of(
                    StandardTypes.INTEGER,
                    StandardTypes.DOUBLE,
                    StandardTypes.DOUBLE,
                    StandardTypes.VARCHAR,
                    StandardTypes.DOUBLE,
                    StandardTypes.DOUBLE);
        }

        if (this.arg0 != null) {
            return ImmutableList.of(
                    StandardTypes.INTEGER,
                    StandardTypes.DOUBLE,
                    StandardTypes.DOUBLE,
                    StandardTypes.VARCHAR,
                    StandardTypes.DOUBLE);
        }

        return ImmutableList.of(
                StandardTypes.INTEGER,
                StandardTypes.DOUBLE,
                StandardTypes.DOUBLE,
                StandardTypes.VARCHAR);
    }

    protected void getSamplesAndWeights(
            int start,
            int length,
            ArrayList<Double> samples,
            ArrayList<Double> weights)
    {
        for (int weight = 1; weight < 3; ++weight) {
            for (int i = start; i < start + length; ++i) {
                final int bin = Math.max(Math.min(i, TestSampleEntropyAggregation.SIZE - 1), 0);
                samples.add(Double.valueOf(bin));
                weights.add(Double.valueOf(weight));
            }
        }
    }

    protected double getEntropyFromSamplesAndWeights(ArrayList<Double> samples, ArrayList<Double> weights)
    {
        final double weight = weights.stream().mapToDouble(c -> c).sum();
        if (weight == 0) {
            return 0;
        }
        final Map<Double, Double> bucketWeights = new HashMap<Double, Double>();
        for (int i = 0; i < samples.size(); ++i) {
            final Double s = samples.get(i);
            final Double w = weights.get(i);
            bucketWeights.put(
                    s,
                    bucketWeights.getOrDefault(s, Double.valueOf(0.0)) + w);
        }
        final double entropy = bucketWeights.values().stream()
                .mapToDouble(w -> w == 0 ? 0 : w / weight * Math.log(weight / w))
                .sum();
        return entropy / Math.log(2);
    }
}
