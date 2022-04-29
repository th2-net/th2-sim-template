package com.exactpro.th2.sim.template.rule.test.examples

import com.exactpro.th2.common.assertInt
import com.exactpro.th2.common.assertString
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.addFields
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.sim.template.rule.KotlinFIXRule
import com.exactpro.th2.sim.template.rule.test.api.TestRuleContext.Companion.testRule
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class FixRuleTest {

    @Test
    fun `negative test`() {
        testRule {
            val rule = KotlinFIXRule(mapOf("check" to "true".toValue()))

            // wrong type of message test
            rule.assertNotTriggered(message("WrongOrder").apply {
                addField("check", "true")
            }.build())
            assertNothingSent()

            // wrong fields test
            rule.assertNotTriggered(message("NewOrderSingle").apply {
                addField("check", "false")
            }.build())
            assertNothingSent()
        }
    }

    @Test
    fun `positive rejected test`() {
        testRule {
            val rule = KotlinFIXRule(mapOf("check" to "true".toValue()))

            // correct type and field check buy without side field
            rule.assertHandle(message("NewOrderSingle").apply {
                addField("check", "true")
            }.build())
            assertSent(Message::class.java) {
                Assertions.assertEquals("Reject", it.messageType)
            }
            assertNothingSent()
        }
    }

    @Test
    fun `INSTR4 test`() {
        KotlinFIXRule.reset()
        // test to check response of message with field SecurityID = INSTR4 and side = 1/2
        testRule {
            val rule = KotlinFIXRule(mapOf("check" to "true".toValue()))

            rule.assertHandle(message("NewOrderSingle").apply {
                addField("check", "true")
                addField("Side", "1")
                addField("SecurityID", "INSTR4")
                addField("OrderQty", 123)
                addField("ClOrdID", "ClOrdID value")
                addField("Price", "Price value")
            }.build())

            assertSent(Message::class.java) { message ->
                Assertions.assertEquals("ExecutionReport", message.messageType)
                message.assertInt("OrderID", 1)
                message.assertInt("ExecID", 1)
            }

            assertSent(Message::class.java) { message ->
                Assertions.assertEquals("ExecutionReport", message.messageType)
                message.assertInt("OrderID", 1)
                message.assertInt("ExecID", 2)
            }

            assertNothingSent()

            rule.assertHandle(message("NewOrderSingle").apply {
                addField("check", "true")
                addFields("Side", "2")
                addFields(
                    "SecurityID", "INSTR4",
                    "OrderQty", 123,
                    "ClOrdID", "ClOrdID value",
                    "Price", "Price value",
                )
            }.build())

            for (i in 0..10) {
                assertSent(Message::class.java) { message ->
                    Assertions.assertEquals("ExecutionReport", message.messageType) { "Execution report with index: $i" }
                }
            }

            assertNothingSent()
        }
    }

    @Test
    fun `INSTR5 test`() {
        KotlinFIXRule.reset()
        // test to check response of message with field SecurityID = INSTR5 and side = 1/2
        testRule {
            val rule = KotlinFIXRule(mapOf("check" to "true".toValue()))

            rule.assertHandle(message("NewOrderSingle").apply {
                addField("check", "true")
                addFields("Side", "1")
                addFields(
                    "SecurityID", "INSTR5",
                    "OrderQty", 123,
                    "ClOrdID", "ClOrdID value",
                    "Price", "Price value",
                )
            }.build())

            assertSent(Message::class.java) { message ->
                Assertions.assertEquals("ExecutionReport", message.messageType)
                message.assertInt("OrderID", 1)
                message.assertInt("ExecID", 1)
            }

            assertSent(Message::class.java) { message ->
                Assertions.assertEquals("ExecutionReport", message.messageType)
                message.assertInt("OrderID", 1)
                message.assertInt("ExecID", 2)
            }

            assertNothingSent()

            rule.assertHandle(message("NewOrderSingle").apply {
                addField("check", "true")
                addFields("Side", "2")
                addFields(
                    "SecurityID", "INSTR5",
                    "OrderQty", 123,
                    "ClOrdID", "ClOrdID value",
                    "Price", "Price value",
                )
            }.build())

            for (i in 0..9) {
                assertSent(Message::class.java) { message ->
                    Assertions.assertEquals("ExecutionReport", message.messageType) { "Execution report with index: $i" }
                }
            }

            assertNothingSent()
        }
    }

    @Test
    fun `INSTR6 test`() {
        KotlinFIXRule.reset()
        // test to check response of message with field SecurityID = INSTR6
        testRule {
            val rule = KotlinFIXRule(mapOf("check" to "true".toValue()))

            rule.assertHandle(message("NewOrderSingle").apply {
                addField("check", "true")
                addFields("Side", "2")
                addFields(
                    "SecurityID", "INSTR6",
                    "OrderQty", 123,
                    "ClOrdID", "ClOrdID value",
                    "Price", "Price value",
                    "BeginString", "BeginString value"
                )
                addField("header", message().apply {
                    addField("MsgSeqNum", 123)
                })
            }.build())

            assertSent(Message::class.java) { message ->
                Assertions.assertEquals("BusinessMessageReject", message.messageType)
                message.assertString("BusinessRejectRefID",  "ClOrdID value")
                message.assertInt("RefSeqNum", 123)
            }

        }
    }
}