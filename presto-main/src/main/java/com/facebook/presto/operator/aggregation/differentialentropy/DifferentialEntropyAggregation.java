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
package com.facebook.presto.operator.aggregation.differentialentropy;

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

@AggregationFunction("differential_entropy")
@Description("Computes differential entropy based on random-variable samples")
public final class DifferentialEntropyAggregation
{
    private DifferentialEntropyAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState State state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method,
            @SqlType(StandardTypes.DOUBLE) double min,
            @SqlType(StandardTypes.DOUBLE) double max)
    {
        final String requestedMethod = method == null ? null : method.toStringUtf8();
        if (state.getStrategy() == null) {
            state.setStrategy(StateSerializer.create(
                    size, requestedMethod, min, max));
        }
        final StateStrategy strategy = state.getStrategy();
        StateSerializer.validate(strategy, requestedMethod);
        strategy.validateParameters(size, sample, weight, min, max);
        strategy.add(sample, weight);
    }

    @InputFunction
    public static void input(
            @AggregationState State state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        final String requestedMethod = method.toStringUtf8();
        if (state.getStrategy() == null) {
            state.setStrategy(StateSerializer.create(
                    size, requestedMethod));
        }
        final StateStrategy strategy = state.getStrategy();
        StateSerializer.validate(strategy, requestedMethod);
        strategy.add(sample, weight);
    }

    @InputFunction
    public static void input(
            @AggregationState State state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        if (state.getStrategy() == null) {
            state.setStrategy(StateSerializer.create(size));
        }
        final StateStrategy strategy = state.getStrategy();
        StateSerializer.validate(strategy);
        strategy.add(sample, weight);
    }

    @InputFunction
    public static void input(
            @AggregationState State state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.DOUBLE) double sample)
    {
        if (state.getStrategy() == null) {
            state.setStrategy(StateSerializer.create(size));
        }
        final StateStrategy strategy = state.getStrategy();
        StateSerializer.validate(strategy);
        strategy.add(sample, 1.0);
    }

    @CombineFunction
    public static void combine(
            @AggregationState State state,
            @AggregationState State otherState)
    {
        final StateStrategy strategy = state.getStrategy();
        final StateStrategy otherStrategy = otherState.getStrategy();
        if (strategy == null && otherStrategy != null) {
            state.setStrategy(otherStrategy.clone());
            return;
        }
        if (strategy != null && otherStrategy != null) {
            StateSerializer.combine(strategy, otherStrategy);
        }
    }

    @OutputFunction("double")
    public static void output(@AggregationState State state, BlockBuilder out)
    {
        final StateStrategy strategy = state.getStrategy();
        if (strategy == null) {
            DOUBLE.writeDouble(out, 0.0);
            return;
        }
        DOUBLE.writeDouble(
                out,
                strategy == null ? 0.0 : strategy.calculateEntropy());
    }
}
