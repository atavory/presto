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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.security.InvalidParameterException;

import static com.facebook.presto.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;

/*
Serializes sample-entropy states.
 */
public class DifferentialEntropyStateSerializer
        implements AccumulatorStateSerializer<DifferentialEntropyState>
{
    public static DifferentialEntropyStateStrategy create(
            long size,
            String method,
            Double arg0,
            Double arg1)
    {
        /*
        Place-holder for reservoir-sampling
        */

        if (method.equalsIgnoreCase("fixed_histogram_mle")) {
            return new DifferentialEntropyStateFixedHistogramMLEStrategy(size, arg0, arg1);
        }

        if (method.equalsIgnoreCase("fixed_histogram_jacknife")) {
            return new DifferentialEntropyStateFixedHistogramJacknifeStrategy(size, arg0, arg1);
        }

        throw new InvalidParameterException(String.format("unknown method %s", method));
    }

    public static void validate(String method, DifferentialEntropyStateStrategy strategy)
    {
        /*
        Place-holder for reservoir-sampling
         */

        if (method.equalsIgnoreCase("fixed_histogram_mle")) {
            if (!(strategy instanceof DifferentialEntropyStateFixedHistogramMLEStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent method");
            }

            return;
        }

        if (method.equalsIgnoreCase("fixed_histogram_jacknife")) {
            if (!(strategy instanceof DifferentialEntropyStateFixedHistogramJacknifeStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent method");
            }

            return;
        }

        throw new InvalidParameterException("unknown method");
    }

    public static void combine(DifferentialEntropyStateStrategy target, DifferentialEntropyStateStrategy source)
    {
        /*
        Place-holder for reservoir-sampling
        }*/

        if (target instanceof DifferentialEntropyStateFixedHistogramMLEStrategy) {
            if (!(source instanceof DifferentialEntropyStateFixedHistogramMLEStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent strategy");
            }
            ((DifferentialEntropyStateFixedHistogramMLEStrategy) target).mergeWith((DifferentialEntropyStateFixedHistogramMLEStrategy) source);
            return;
        }

        if (target instanceof DifferentialEntropyStateFixedHistogramJacknifeStrategy) {
            if (!(source instanceof DifferentialEntropyStateFixedHistogramJacknifeStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent strategy");
            }
            ((DifferentialEntropyStateFixedHistogramJacknifeStrategy) target).mergeWith((DifferentialEntropyStateFixedHistogramJacknifeStrategy) source);
            return;
        }

        throw new InvalidParameterException("unknown strategy combination");
    }

    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(DifferentialEntropyState state, BlockBuilder out)
    {
        final int requiredBytes =
                SizeOf.SIZE_OF_INT + // Method
                        (state.getStrategy() == null ? 0 : state.getStrategy().getRequiredBytesForSerialization());

        final DifferentialEntropyStateStrategy strategy = state.getStrategy();

        SliceOutput sliceOut = Slices.allocate(requiredBytes).getOutput();

        if (strategy == null) {
            sliceOut.appendInt(0);
        }
        /*
        Placeholder for reservoir sampling.
         */
        else if (strategy instanceof DifferentialEntropyStateFixedHistogramMLEStrategy) {
            sliceOut.appendInt(2);
        }
        else if (strategy instanceof DifferentialEntropyStateFixedHistogramJacknifeStrategy) {
            sliceOut.appendInt(3);
        }
        else {
            throw new InvalidParameterException("unknown method in serialize");
        }

        if (strategy != null) {
            strategy.serialize(sliceOut);
        }

        VARBINARY.writeSlice(out, sliceOut.getUnderlyingSlice());
    }

    @Override
    public void deserialize(
            Block block,
            int index,
            DifferentialEntropyState state)
    {
        DifferentialEntropyStateSerializer.deserialize(
                VARBINARY.getSlice(block, index).getInput(),
                state);
    }

    public static void deserialize(
            SliceInput input,
            DifferentialEntropyState state)
    {
        final DifferentialEntropyStateStrategy strategy =
                DifferentialEntropyStateSerializer.deserializeStrategy(input);
        if (strategy == null && state.getStrategy() != null) {
            throw new PrestoException(
                    CONSTRAINT_VIOLATION,
                    "strategy is not null for null method");
        }
        if (strategy != null) {
            state.setStrategy(strategy);
        }
    }

    public static DifferentialEntropyStateStrategy deserializeStrategy(SliceInput input)
    {
        final int method = input.readInt();
        if (method == 0) {
            return null;
        }
        /*
        Place-holder for reservoir-sampling
         */
        if (method == 2) {
            return new DifferentialEntropyStateFixedHistogramMLEStrategy(input);
        }
        if (method == 3) {
            return new DifferentialEntropyStateFixedHistogramJacknifeStrategy(input);
        }
        throw new InvalidParameterException("unknown method in deserialize");
    }
}
