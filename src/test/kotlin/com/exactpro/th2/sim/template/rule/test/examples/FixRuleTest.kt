/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.sim.template.FixFields
import com.exactpro.th2.sim.template.rule.KotlinFIXRule
import com.exactpro.th2.sim.template.rule.test.api.TestRuleContext.Companion.testRule
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class FixRuleTest {

    @Test
    fun `negative test`() {
        testRule {
            val rule = KotlinFIXRule(mapOf("check" to "true"))

            // wrong type of message test
            rule.assertNotTriggered(message("WrongOrder") {
                addField("check", "true")
            }.build())
            assertNothingSent()

            // wrong fields test
            rule.assertNotTriggered(message("NewOrderSingle") {
                addField("check", "false")
            }.build())
            assertNothingSent()
        }
    }

    @Test
    fun `positive rejected test`() {
        testRule {
            val rule = KotlinFIXRule(mapOf("check" to "true"))

            // correct type and field check buy without side field
            rule.assertHandle(message("NewOrderSingle").apply {
                addField("check", "true")
            }.build())
            assertSent(ParsedMessage.FromMapBuilder::class.java) {
                Assertions.assertEquals("Reject", it.type)
            }
            assertNothingSent()
        }
    }

    @Test
    fun `INSTR4 test`() {
        // test to check response of message with field SecurityID = INSTR4 and side = 1/2
        testRule {
            val rule = KotlinFIXRule(mapOf("check" to "true"))
            KotlinFIXRule.reset()

            for (i in 0..1) {
                rule.assertHandle(message("NewOrderSingle").apply {
                    addField("check", "true")
                    addField(FixFields.SIDE, "1")
                    addField(FixFields.SECURITY_ID, "INSTR4")
                    addField(FixFields.ORDER_QTY, 123)
                    addField(FixFields.CL_ORD_ID, "ClOrdID value")
                    addField(FixFields.PRICE, "Price value")
                }.build())

                assertSent(ParsedMessage.FromMapBuilder::class.java) { message ->
                    Assertions.assertEquals("ExecutionReport", message.type)
                    assertEquals(i + 1, message.bodyBuilder()[FixFields.ORDER_ID])
                    assertEquals(2 * i + 1, message.bodyBuilder()[FixFields.EXEC_ID])
                }

                assertSent(ParsedMessage.FromMapBuilder::class.java) { message ->
                    Assertions.assertEquals("ExecutionReport", message.type)
                    assertEquals(i + 1, message.bodyBuilder()[FixFields.ORDER_ID])
                    assertEquals(2 * i + 2, message.bodyBuilder()[FixFields.EXEC_ID])
                }

                assertNothingSent()
            }

            rule.assertHandle(message("NewOrderSingle").apply {
                addFields(
                    "check" to "true",
                    FixFields.SIDE to "2",
                    FixFields.SECURITY_ID to "INSTR4",
                    FixFields.ORDER_QTY to 123,
                    FixFields.CL_ORD_ID to "ClOrdID value",
                    FixFields.PRICE to "Price value",
                )
            }.build())

            for (i in 0..10) {
                assertSent(ParsedMessage.FromMapBuilder::class.java) { message ->
                    Assertions.assertEquals("ExecutionReport", message.type) { "Execution report with index: $i" }
                }
            }

            assertNothingSent()
        }
    }

    @Test
    fun `INSTR5 test`() {
        // test to check response of message with field SecurityID = INSTR5 and side = 1/2
        testRule {
            val rule = KotlinFIXRule(mapOf("check" to "true"))
            KotlinFIXRule.reset()

            for (i in 0..1) {
                rule.assertHandle(message("NewOrderSingle") {
                    addFields(
                        "check" to "true",
                        FixFields.SIDE to "1",
                        FixFields.SECURITY_ID to "INSTR5",
                        FixFields.ORDER_QTY to 123,
                        FixFields.CL_ORD_ID to "ClOrdID value",
                        FixFields.PRICE to "Price value",
                    )
                }.build())

                assertSent(ParsedMessage.FromMapBuilder::class.java) { message ->
                    Assertions.assertEquals("ExecutionReport", message.type)
                    assertEquals(i + 1, message.bodyBuilder()[FixFields.ORDER_ID])
                    assertEquals(2 * i + 1, message.bodyBuilder()[FixFields.EXEC_ID])
                }

                assertSent(ParsedMessage.FromMapBuilder::class.java) { message ->
                    Assertions.assertEquals("ExecutionReport", message.type)
                    assertEquals(i + 1, message.bodyBuilder()[FixFields.ORDER_ID])
                    assertEquals(2 * i + 2, message.bodyBuilder()[FixFields.EXEC_ID])
                }

                assertNothingSent()
            }

            rule.assertHandle(message("NewOrderSingle") {
                addFields(
                    "check" to "true",
                    FixFields.SIDE to "2",
                    FixFields.SECURITY_ID to "INSTR5",
                    FixFields.ORDER_QTY to 123,
                    FixFields.CL_ORD_ID to "ClOrdID value",
                    FixFields.PRICE to "Price value",
                )
            }.build())

            for (i in 0..9) {
                assertSent(ParsedMessage.FromMapBuilder::class.java) { message ->
                    Assertions.assertEquals("ExecutionReport", message.type) { "Execution report with index: $i" }
                }
            }

            assertNothingSent()
        }
    }

    @Test
    fun `INSTR6 test`() {
        // test to check response of message with field SecurityID = INSTR6
        testRule {
            val rule = KotlinFIXRule(mapOf("check" to "true"))
            KotlinFIXRule.reset()

            rule.assertHandle(message("NewOrderSingle") {
                addFields(
                    "check" to "true",
                    FixFields.SIDE to "2",
                    FixFields.SECURITY_ID to "INSTR6",
                    FixFields.ORDER_QTY to 123,
                    FixFields.CL_ORD_ID to "ClOrdID value",
                    FixFields.PRICE to "Price value",
                    FixFields.BEGIN_STRING to "BeginString value",
                    "header" to hashMapOf(
                        FixFields.MSG_SEQ_NUM to 123
                    )
                )
            }.build())

            assertSent(ParsedMessage.FromMapBuilder::class.java) { message ->
                Assertions.assertEquals("BusinessMessageReject", message.type)
                assertEquals("ClOrdID value", message.bodyBuilder()[FixFields.BUSINESS_REJECT_REF_ID])
                assertEquals(123, message.bodyBuilder()[FixFields.REF_SEQ_NUM])
            }
        }
    }
}