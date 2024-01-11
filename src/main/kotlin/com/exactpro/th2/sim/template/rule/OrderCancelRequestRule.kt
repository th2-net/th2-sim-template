/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.sim.template.rule

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.message
import com.exactpro.th2.common.utils.message.transport.containsField
import com.exactpro.th2.common.utils.message.transport.addFields
import com.exactpro.th2.common.utils.message.transport.copyFields
import com.exactpro.th2.common.utils.message.transport.getField
import com.exactpro.th2.common.utils.message.transport.getFieldSoft
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessagePredicateRule

class DemoCancelRequestRule(field: Map<String, Any?>) : MessagePredicateRule() {
    private val alias = "fix-demo-server1"

    init {
        init(
            { it in setOf("OrderCancelRequest", "OrderCancelReplaceRequest") },
            emptyMap()
        )
    }

    override fun handle(context: IRuleContext, incomeMessage: ParsedMessage) {
        val response = if (!incomeMessage.containsField("SecurityID")) {
            message("BusinessMessageReject").addFields(
                "RefTagID" to "48",
                "RefMsgType" to "f",
                "RefSeqNum" to incomeMessage.getFieldSoft("BeginString", "MsgSeqNum"),
                "Text" to "Incorrect instrument",
                "SessionRejectReason" to "99"
            )
        } else {
            val builder = message("ExecutionReport")
                .copyFields(incomeMessage, "ClOrdID", "SecondaryClOrdID", "Symbol", "Side", "TransactTime")

            when (incomeMessage.getField("header", "MsgType")) {
                "F" -> builder // OrderCancelRequest
                    .addFields(
                        "OrdStatus" to "5", // "Cancelled"
                        "Text" to "The simulated order has been cancelled"
                    )

                "G" -> builder // OrderCancelReplaceRequest
                    .addFields(
                        "OrdStatus" to "3", // "Replaced"
                        "Text" to "The simulated order has been replaced"
                    )

                else -> error("wrong MsgType")
            }
        }
        response.idBuilder().setSessionAlias(alias)
        context.send(response)
    }
}