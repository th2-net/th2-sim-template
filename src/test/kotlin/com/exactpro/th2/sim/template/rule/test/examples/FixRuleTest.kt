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
                    addField("Side", "1")
                    addField("SecurityID", "INSTR4")
                    addField("OrderQty", 123)
                    addField("ClOrdID", "ClOrdID value")
                    addField("Price", "Price value")
                }.build())

                assertSent(ParsedMessage.FromMapBuilder::class.java) { message ->
                    Assertions.assertEquals("ExecutionReport", message.type)
                    assertEquals(i + 1, message.bodyBuilder()["OrderID"])
                    assertEquals(2 * i + 1, message.bodyBuilder()["ExecID"])
                }

                assertSent(ParsedMessage.FromMapBuilder::class.java) { message ->
                    Assertions.assertEquals("ExecutionReport", message.type)
                    assertEquals(i + 1, message.bodyBuilder()["OrderID"])
                    assertEquals(2 * i + 2, message.bodyBuilder()["ExecID"])
                }

                assertNothingSent()
            }

            rule.assertHandle(message("NewOrderSingle").apply {
                addFields(
                    "check" to "true",
                    "Side" to "2",
                    "SecurityID" to "INSTR4",
                    "OrderQty" to 123,
                    "ClOrdID" to "ClOrdID value",
                    "Price" to "Price value",
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
                        "Side" to "1",
                        "SecurityID" to "INSTR5",
                        "OrderQty" to 123,
                        "ClOrdID" to "ClOrdID value",
                        "Price" to "Price value",
                    )
                }.build())

                assertSent(ParsedMessage.FromMapBuilder::class.java) { message ->
                    Assertions.assertEquals("ExecutionReport", message.type)
                    assertEquals(i + 1, message.bodyBuilder()["OrderID"])
                    assertEquals(2 * i + 1, message.bodyBuilder()["ExecID"])
                }

                assertSent(ParsedMessage.FromMapBuilder::class.java) { message ->
                    Assertions.assertEquals("ExecutionReport", message.type)
                    assertEquals(i + 1, message.bodyBuilder()["OrderID"])
                    assertEquals(2 * i + 2, message.bodyBuilder()["ExecID"])
                }

                assertNothingSent()
            }

            rule.assertHandle(message("NewOrderSingle") {
                addFields(
                    "check" to "true",
                    "Side" to "2",
                    "SecurityID" to "INSTR5",
                    "OrderQty" to 123,
                    "ClOrdID" to "ClOrdID value",
                    "Price" to "Price value",
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
                    "Side" to "2",
                    "SecurityID" to "INSTR6",
                    "OrderQty" to 123,
                    "ClOrdID" to "ClOrdID value",
                    "Price" to "Price value",
                    "BeginString" to "BeginString value",
                    "header" to hashMapOf(
                        "MsgSeqNum" to 123
                    )
                )
            }.build())

            assertSent(ParsedMessage.FromMapBuilder::class.java) { message ->
                Assertions.assertEquals("BusinessMessageReject", message.type)
                assertEquals("ClOrdID value", message.bodyBuilder()["BusinessRejectRefID"])
                assertEquals(123, message.bodyBuilder()["RefSeqNum"])
            }

        }
    }
}