/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.sim.template.FixFields

class DemoCancelRequestRule(field: Map<String, Any?>) : MessagePredicateRule() {
    private val alias = "fix-demo-server1"

    init {
        init(
            { it in setOf("OrderCancelRequest", "OrderCancelReplaceRequest") },
            emptyMap()
        )
    }

    override fun handle(context: IRuleContext, incomeMessage: ParsedMessage) {
        val response = if (!incomeMessage.containsField(FixFields.SECURITY_ID)) {
            message("BusinessMessageReject").addFields(
                FixFields.REF_TAG_ID to "48",
                FixFields.REF_MSG_TYPE to "f",
                FixFields.REF_SEQ_NUM to incomeMessage.getFieldSoft(FixFields.BEGIN_STRING, FixFields.MSG_SEQ_NUM),
                FixFields.TEXT to "Incorrect instrument",
                FixFields.SESSION_REJECT_REASON to "99"
            )
        } else {
            val builder = message("ExecutionReport")
                .copyFields(incomeMessage, FixFields.CL_ORD_ID, FixFields.SECONDARY_CL_ORD_ID, FixFields.SYMBOL, FixFields.SIDE, FixFields.TRANSACT_TIME)

            when (incomeMessage.getField("header", FixFields.MSG_TYPE)) {
                "F" -> builder // OrderCancelRequest
                    .addFields(
                        FixFields.ORD_STATUS to "5", // "Cancelled"
                        FixFields.TEXT to "The simulated order has been cancelled"
                    )

                "G" -> builder // OrderCancelReplaceRequest
                    .addFields(
                        FixFields.ORD_STATUS to "3", // "Replaced"
                        FixFields.TEXT to "The simulated order has been replaced"
                    )

                else -> error("wrong MsgType")
            }
        }
        response.idBuilder().setSessionAlias(alias)
        context.send(response)
    }
}