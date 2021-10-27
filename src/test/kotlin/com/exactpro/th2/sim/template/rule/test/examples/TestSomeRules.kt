package com.exactpro.th2.sim.template.rule.test.examples ///*******************************************************************************
// * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// ******************************************************************************/
//package com.exactpro.th2.sim.template.rule.test
//
//import com.exactpro.th2.common.grpc.MessageBatch
//import com.exactpro.th2.common.message.addFields
//import com.exactpro.th2.common.message.message
//import com.exactpro.th2.common.value.toValue
//import com.exactpro.th2.sim.ISimulator
//import com.exactpro.th2.sim.rule.test.AbstractRuleTest
//import com.exactpro.th2.sim.template.rule.TemplateFixRule
//
//class TestSomeRules : AbstractRuleTest() {
//
//
//    override fun createMessageBatch(index: Int): MessageBatch? = MessageBatch.newBuilder().addMessages(message("NewOrderSingle").apply {
//        if (index % 4 == 0) {
//            addFields("ClOrdId", "ord_1")
//        }
//        else {
//            addFields("ClOrdId", "ord_2")
//        }
//    } .build()).build()
//
//    override fun getCountMessageBatches(): Int = 1000
//
//    override fun addRules(simulator: ISimulator, sessionAlias: String) {
//        simulator.apply {
//            addRule(TemplateFixRule(mapOf("ClOrdId" to "ord_1".toValue())), sessionAlias)
//            addRule(TemplateFixRule(mapOf("ClOrdId" to "ord_2".toValue())), sessionAlias)
//        }
//    }
//
//    override fun getPathLoggingFile(): String? = "./output.csv"
//
//    override fun checkResultMessages(index: Int, messageBatches: MutableList<MessageBatch>): Boolean = (index % 4 != 0 || messageBatches.size == 1 && messageBatches[0].messagesCount == 1 && messageBatches[0].messagesList[0].metadata.messageType == "ExecutionReport")
//
//}