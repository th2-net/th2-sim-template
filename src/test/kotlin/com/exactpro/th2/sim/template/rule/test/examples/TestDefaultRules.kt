/////*******************************************************************************
//// * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
//// *
//// * Licensed under the Apache License, Version 2.0 (the "License");
//// * you may not use this file except in compliance with the License.
//// * You may obtain a copy of the License at
//// *
//// *     http://www.apache.org/licenses/LICENSE-2.0
//// *
//// * Unless required by applicable law or agreed to in writing, software
//// * distributed under the License is distributed on an "AS IS" BASIS,
//// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// * See the License for the specific language governing permissions and
//// * limitations under the License.
//// ******************************************************************************/
//package com.exactpro.th2.sim.template.rule.test.examples
//
//import com.exactpro.th2.common.message.addFields
//import com.exactpro.th2.common.message.message
//import com.exactpro.th2.common.value.toValue
//import com.exactpro.th2.sim.template.rule.TemplateFixRule
//import com.exactpro.th2.sim.template.rule.test.api.TestRuleContext
//import org.junit.jupiter.api.Test
//import java.time.LocalDateTime
//
//class TestDefaultRules {
//
//    private val testContext = TestRuleContext()
//
//    @Test
//    fun orderOne() {
//        testContext.test {
//            val rule = TemplateFixRule(mapOf("ClOrdId" to "order_id_1".toValue()))
//            val testMsg = message("NewOrderSingle").apply {
//                addFields("ClOrdId", "order_id_1", "1", "1", "2", "2")
//            }.build()
//            rule.assertTriggered(testMsg)
//            val expectedMessage = message("ExecutionReport").addFields(
//                "OrderID", 1,
//                "ExecID", 1,
//                "ExecType", "2",
//                "OrdStatus", "0",
//                "CumQty", "0",
//                "TradingParty", null,
//                "TransactTime", LocalDateTime.now().toString()
//            ).build()
//            assertSent(expectedMessage)
//        }
//    }
//}