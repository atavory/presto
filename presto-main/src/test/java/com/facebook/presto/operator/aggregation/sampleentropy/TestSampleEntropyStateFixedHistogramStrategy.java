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

import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class TestSampleEntropyStateFixedHistogramStrategy
{
    private StrategyCreator strategyCreator;

    protected static interface StrategyCreator
    {
        public SampleEntropyStateStrategy create(int bucketCount, double min, double max);
    }

    public TestSampleEntropyStateFixedHistogramStrategy(StrategyCreator strategyCreator)
    {
        this.strategyCreator = strategyCreator;
    }

    @Test
    public void getters()
    {
        final SampleEntropyStateStrategy strategy =
                strategyCreator.create(200, 0.0, 1.1);
    }

    @Test
    public void illegalBucketCount()
    {
        try {
            strategyCreator.create(-200, 3.0, 4.1);
            fail("Exception expected");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("bucketcount"));
        }
    }

    @Test
    public void illegalMinMax()
    {
        try {
            strategyCreator.create(200, 3.0, 3.0);
            fail("Exception expected");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("min"));
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("max"));
        }
    }

    @Test
    public void uniform()
    {
        final SampleEntropyStateStrategy strategy =
                strategyCreator.create(200, 0.0, 10.0);
        final Random random = new Random(13);
        for (int i = 0; i < 9999999; ++i) {
            strategy.add(10 * random.nextFloat(), 1.0);
        }
        assertEquals(
                strategy.calculateEntropy(),
                Math.log(10) / Math.log(2),
                0.001);
    }

    @Test
    public void outOfBounds()
    {
        final SampleEntropyStateStrategy strategy =
                strategyCreator.create(200, 0.0, 10.0);
        final Random random = new Random(13);
        try {
            strategy.add(-1, 1.0);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("range"));
        }
    }

    @Test
    public void negativeWeight()
    {
        final SampleEntropyStateStrategy strategy =
                strategyCreator.create(200, 0.0, 10.0);
        try {
            strategy.validateParams(200, 0.0, -1.0, 0.0, 10.0);
            fail("Expected exception");
        }
        catch (PrestoException e) {
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("weight"));
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("negative"));
        }
    }
}
