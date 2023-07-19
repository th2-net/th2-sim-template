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

package com.exactpro.th2.sim.template.rule

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.addFields
import com.exactpro.th2.common.utils.message.transport.containsField
import com.exactpro.th2.common.utils.message.transport.getFieldSoft
import com.exactpro.th2.common.utils.message.transport.getString
import com.exactpro.th2.common.utils.message.transport.message
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule

class KotlinFIXRuleSecurity(field: Map<String, Any?>) : MessageCompareRule() {

    init {
        init("SecurityStatusRequest", field)
    }

    override fun handle(context: IRuleContext, incomeMessage: ParsedMessage) {
        if (!incomeMessage.containsField("SecurityID")) {
            val reject = message("Reject").addFields(
                "RefTagID" to "48",
                "RefMsgType" to "f",
                "RefSeqNum" to incomeMessage.getFieldSoft("BeginString", "MsgSeqNum"),
                "Text" to "Incorrect instrument",
                "SessionRejectReason" to "99"
            )
            context.send(reject)
        } else {
            if (incomeMessage.getString("SecurityID") == "INSTR6") {
                val unknownInstr = message("SecurityStatus").addFields(
                    "SecurityID" to incomeMessage.getString("SecurityID")!!,
                    "SecurityIDincomeMessage" to incomeMessage.getString("SecurityIDincomeMessage")!!,
                    "SecurityStatusReqID" to incomeMessage.getString("SecurityStatusReqID")!!,
                    "UnsolicitedIndicator" to "N",
                    "SecurityTradingStatus" to "20",
                    "Text" to "Unknown or Invalid instrument"
                )
                context.send(unknownInstr)
            } else {
                val securityStatus1 = message("SecurityStatus").addFields(
                    "SecurityID" to incomeMessage.getString("SecurityID")!!,
                    "SecurityIDincomeMessage" to incomeMessage.getString("SecurityIDincomeMessage")!!,
                    "SecurityStatusReqID" to incomeMessage.getString("SecurityStatusReqID")!!,
                    "Currency" to "RUB",
                    "MarketID" to "Demo Market",
                    "MarketSegmentID" to "NEW",
                    "TradingSessionID" to "1",
                    "TradingSessionSubID" to "3",
                    "UnsolicitedIndicator" to "N",
                    "SecurityTradingStatus" to "17",
                    "BuyVolume" to "0",
                    "SellVolume" to "0",
                    "HighPx" to "56",
                    "LowPx" to "54",
                    "LastPx" to "54",
                    "FirstPx" to "54",
                    "Text" to "The simulated SecurityStatus has been sent"
                )
                context.send(securityStatus1)
            }
        }
    }
}
