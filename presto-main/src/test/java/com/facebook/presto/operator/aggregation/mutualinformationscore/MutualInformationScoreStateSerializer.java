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
package com.facebook.presto.operator.aggregation.mutualinformationscore;

import com.facebook.presto.operator.aggregation.sampleentropy.SampleEntropyState;
import com.facebook.presto.operator.aggregation.sampleentropy.SampleEntropyStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;

public class MutualInformationScoreStateSerializer
        implements AccumulatorStateSerializer<SampleEntropyState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(MutualInformationScoreState state, BlockBuilder out)
    {
        final DifferentialEntropyMap entropyMap = state.getEntropyMap();
        int requiredBytes = SizeOf.SIZE_OF_LONG; // Outcome cardinality

        if (entropyMap == 0) {
            Slices.allocate(requiredBytes).getOutput().appendLong(0);

            return;
        }

        requiredBytes += entropyMap.entropyStrategy.getRequiredBytesForSerialization();
        requiredBytes += entropyMap.outcomeEntropyStrategies.values()
                .stream()
                .mapToLong(m -> SizeOf.SIZE_OF_LONG + m.getRequiredBytesForSerialization())
                .sum();

        SliceOutput out = Slices.allocate(requiredBytes).getOutput();
        entropyMap.entropyStrategy.serialize(out);
        entropyMap.outcomeEntropyStrategies.entrySet()
                .stream()
                .forEach(e -> {
                    out.appendLong(e.getKey());
                    e.getValue().serialize(out);
                });

        VARBINARY.writeSlice(out, out.getUnderlyingSlice());
    }

    @Override
    public void deserialize(
            Block block,
            int index,
            MutualInformationScoreState state)
    {
        final SliceInput input = VARBINARY.getSlice(block, index).getInput();
        final DifferentialEntropyMap entropyMap = new DifferentialEntropyMap();
        final long numOutcomes = input.readLong();
        if (numOutcomes > 0) {
            entropyMap.entropyStrategy = new SampleEntropyStateSerializer().deserialize();
            while (numOutcomes-- > 0) {
                entropyMap.outcomeEntropyStrategies.put(
                        input.readLong(),
                        SampleEntropyStateSerializer.deserialize(input));
            }
        }
        state.setEntropyMap(entropyMap);
    }
}
