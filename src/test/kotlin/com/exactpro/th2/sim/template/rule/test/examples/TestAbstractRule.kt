package com.exactpro.th2.sim.template.rule.test.examples

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.message.addFields
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.sim.template.rule.TemplateAbstractRule
import com.exactpro.th2.sim.template.rule.test.api.TestRuleContext
import com.exactpro.th2.sim.template.rule.test.api.testRule
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestAbstractRule {
    private val testContext = TestRuleContext()

    @Test
    fun `simple triggered test`() {
        testContext.test {
            val rule = TemplateAbstractRule()
            rule.assertTriggered(/* test input message */ message("NewOrderSingle").apply {
                addFields("field1", 45, "field2", 45, "field3", "field3 test value")
            }.build())
            assertSent(/* expected output message */ message("ExecutionReport").addFields(
                "field1", 45,
                "field3", "field3 test value",
                "field4", "value"
            ).build())
        }
    }

    @Test
    fun `simple not triggered test`() {
        testContext.test {
            val rule = TemplateAbstractRule()
            val testMsg = message("NewOrderSingle").apply {
                addFields("field1", 1, "field2", 2, "field3", "field3 test value")
            }.build()
            rule.assertNotTriggered(testMsg)
            assertNothingSent()
        }
    }

    @Test
    fun `custom triggered test`() {
        testContext.test {
            val rule = TemplateAbstractRule()
            rule.assertTriggered(/* test input message */ message("NewOrderSingle").apply {
                addFields("field1", 45, "field2", 45, "field3", "field3 test value")
            }.build())
            assertSent(Message::class.java) { actual:  Message ->
                Assertions.assertEquals(actual.messageType , "ExecutionReport")
            }
        }
    }

    @Test
    fun `custom triggered static test`() {
        testRule {
            val rule = TemplateAbstractRule()
            rule.assertTriggered(/* test input message */ message("NewOrderSingle").apply {
                addFields("field1", 45, "field2", 45, "field3", "field3 test value")
            }.build())
            assertSent(Message::class.java) { actual:  Message ->
                Assertions.assertEquals(actual.messageType , "ExecutionReport")
            }
        }
    }
}