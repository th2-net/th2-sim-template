/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
 *
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
package com.exactpro.th2.sim.template.rule.test.examples

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.addFields
import com.exactpro.th2.common.utils.message.transport.message
import com.exactpro.th2.sim.template.rule.TemplateAbstractRule
import com.exactpro.th2.sim.template.rule.test.api.TestRuleContext.Companion.testRule
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestAbstractRule {

    @Test
    fun `simple triggered test`() {
        testRule {
            val rule = TemplateAbstractRule()
            rule.assertHandle(/* test input message */ message("NewOrderSingle")
                .addFields(
                    "field1" to 45,
                    "field2" to 45,
                    "field3" to "field3 test value",
                ).build()
            )
            assertSent(/* expected output message */ message("ExecutionReport")
                .addFields(
                    "field1" to 45,
                    "field3" to "field3 test value",
                    "field4" to "value",
                )
            )
        }
    }

    @Test
    fun `simple not triggered test`() {
        testRule {
            val rule = TemplateAbstractRule()
            val testMsg = message("NewOrderSingle") {
                addFields(
                    "field1" to 1,
                    "field2" to 2,
                    "field3" to "field3 test value"
                )
            }.build()
            rule.assertNotTriggered(testMsg)
            assertNothingSent()
        }
    }


    @Test
    fun `custom triggered  test`() {
        testRule {
            val rule = TemplateAbstractRule()
            rule.assertHandle(/* test input message */ message("NewOrderSingle") {
                addFields(
                    "field1" to 45,
                    "field2" to 45,
                    "field3" to "field3 test value"
                )
            }.build())
            assertSent(ParsedMessage.FromMapBuilder::class.java) { actual ->
                Assertions.assertEquals(actual.type, "ExecutionReport")
            }
        }
    }
}