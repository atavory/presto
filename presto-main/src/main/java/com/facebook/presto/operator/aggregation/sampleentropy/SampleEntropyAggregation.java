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

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("sample_entropy")
@Description("Computes entropy based on random-variable samples")
public final class SampleEntropyAggregation
{
    private static final Double DEFAULT_WEIGHT = 1.0;
    private static final String DEFAULT_METHOD = "mle";

    private SampleEntropyAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState SampleEntropyState state,
            @SqlType(StandardTypes.BIGINT) long bucketCount,
            @SqlType(StandardTypes.DOUBLE) double min,
            @SqlType(StandardTypes.DOUBLE) double max,
            @SqlType(StandardTypes.DOUBLE) double value,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        if (state.getStrategy() == null) {
            state.setStrategy(SampleEntropyStateSerializer.create(
                    method.toStringUtf8(), bucketCount, min, max));
        }
        final SampleEntropyStateStrategy strategy = state.getStrategy();
        SampleEntropyStateSerializer.validate(method.toStringUtf8(), strategy);
        strategy.validateParams(bucketCount, min, max, weight);
        final Double effectiveValue = Math.max(Math.min(value, max), min);
        strategy.add(effectiveValue, weight);
    }

    /*
    @InputFunction
    public static void input(
            @AggregationState SampleEntropyState state,
            @SqlType(StandardTypes.BIGINT) long bucketCount,
            @SqlType(StandardTypes.DOUBLE) double min,
            @SqlType(StandardTypes.DOUBLE) double max,
            @SqlType(StandardTypes.DOUBLE) double value,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        input(
                state,
                bucketCount,
                min,
                max,
                value,
                weight,
                SampleEntropyAggregation.DEFAULT_METHOD);
    }

    @InputFunction
    public static void input(
            @AggregationState SampleEntropyState state,
            @SqlType(StandardTypes.BIGINT) long bucketCount,
            @SqlType(StandardTypes.DOUBLE) double min,
            @SqlType(StandardTypes.DOUBLE) double max,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        SampleEntropyAggregation.input(
                state,
                bucketCount,
                min,
                max,
                value,
                SampleEntropyAggregation.DEFAULT_WEIGHT);
    }
     */

    @CombineFunction
    public static void combine(
            @AggregationState SampleEntropyState state,
            @AggregationState SampleEntropyState otherState)
    {
        final SampleEntropyStateStrategy strategy = state.getStrategy();
        final SampleEntropyStateStrategy otherStrategy = otherState.getStrategy();
        if (strategy == null && otherStrategy != null) {
            state.setStrategy(otherStrategy.clone());
            return;
        }
        if (strategy != null && otherStrategy != null) {
            SampleEntropyStateSerializer.combine(strategy, otherStrategy);
        }
    }

    @OutputFunction("double")
    public static void output(@AggregationState SampleEntropyState state, BlockBuilder out)
    {
        final SampleEntropyStateStrategy strategy = state.getStrategy();
        if (strategy == null) {
            DOUBLE.writeDouble(out, 0.0);
            return;
        }
        DOUBLE.writeDouble(
                out,
                strategy == null ? 0.0 : strategy.calculateEntropy());
    }
}
