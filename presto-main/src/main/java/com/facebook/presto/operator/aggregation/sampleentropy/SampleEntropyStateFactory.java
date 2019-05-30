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

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

import static java.util.Objects.requireNonNull;

public class SampleEntropyStateFactory
        implements AccumulatorStateFactory<SampleEntropyState>
{
    @Override
    public SampleEntropyState createSingleState()
    {
        return new SingleState();
    }

    @Override
    public Class<? extends SampleEntropyState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public SampleEntropyState createGroupedState()
    {
        return new GroupedState();
    }

    @Override
    public Class<? extends SampleEntropyState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements SampleEntropyState
    {
        private final ObjectBigArray<SampleEntropyStateStrategy> strategies = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            strategies.ensureCapacity(size);
        }

        @Override
        public void setStrategy(SampleEntropyStateStrategy strategy)
        {
            requireNonNull(strategy, "strategy is null");

            SampleEntropyStateStrategy previous = getStrategy();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            strategies.set(getGroupId(), strategy);
            size += strategy.estimatedInMemorySize();
        }

        @Override
        public SampleEntropyStateStrategy getStrategy()
        {
            return strategies.get(getGroupId());
        }

        @Override
        public long getEstimatedSize()
        {
            return size + strategies.sizeOf();
        }
    }

    public static class SingleState
            implements SampleEntropyState
    {
        private SampleEntropyStateStrategy strategy;

        @Override
        public void setStrategy(SampleEntropyStateStrategy strategy)
        {
            requireNonNull(strategy, "strategy is null");

            this.strategy = strategy;
        }

        @Override
        public SampleEntropyStateStrategy getStrategy()
        {
            return strategy;
        }

        @Override
        public long getEstimatedSize()
        {
            if (strategy == null) {
                return 0;
            }
            return strategy.estimatedInMemorySize();
        }
    }
}
