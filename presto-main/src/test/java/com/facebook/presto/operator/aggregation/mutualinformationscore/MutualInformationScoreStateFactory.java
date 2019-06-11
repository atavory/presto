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

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.sql.planner.plan.Patterns;

import static java.util.Objects.requireNonNull;

public class MutualInformationScoreStateFactory
        implements AccumulatorStateFactory<MutualInformationScoreState>
{
    @Override
    public MutualInformationScoreState createSingleState()
    {
        return new SingleState();
    }

    @Override
    public Class<? extends MutualInformationScoreState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public MutualInformationScoreState createGroupedState()
    {
        return new GroupedState();
    }

    @Override
    public Class<? extends MutualInformationScoreState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements MutualInformationScoreState
    {
        private final ObjectBigArray<DifferentialEntropyMap> maps = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            maps.ensureCapacity(size);
        }

        @Override
        public void set(DifferentialEntropyMap entropyMap)
        {
            requireNonNull(entropyMap, "entropyMap is null");

            DifferentialEntropyMap previous = get();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            maps.set(getGroupId(), entropyMap);
            size += entropyMap.estimatedInMemorySize();
        }

        @Override
        public DifferentialEntropyMap get()
        {
            return maps.get(getGroupId());
        }

        @Override
        public long getEstimatedSize()
        {
            return size + maps.sizeOf();
        }
    }

    public static class SingleState
            implements MutualInformationScoreState
    {
        private DifferentialEntropyMap entropyMap;

        @Override
        public void set(DifferentialEntropyMap entropyMap)
        {
            requireNonNull(entropyMap, "entropyMap is null");

            this.entropyMap = entropyMap;
        }

        @Override
        public Patterns.Sample get()
        {
            return entropyMap;
        }

        @Override
        public long getEstimatedSize()
        {
            if (entropyMap == null) {
                return 0;
            }
            return entropyMap.estimatedInMemorySize();
        }
    }
}
