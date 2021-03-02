/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/

package com.exactpro.th2.simulator.template.rule

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.*
import com.exactpro.th2.common.value.getMessage
import com.exactpro.th2.common.value.getString
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule

import java.util.concurrent.atomic.AtomicInteger

class KotlinFIXRuleSecurity(field: Map<String, Value>) : MessageCompareRule() {

    companion object{
    private var orderId = AtomicInteger(0)
    private var execId = AtomicInteger(0)
    }

    init {
        init("SecurityStatusRequest", field)
    }

    override fun handle(context: IRuleContext, incomeMessage: Message) {
        if (!incomeMessage.containsFields("SecurityID")){
            val reject = message("Reject").addFields(
                    "RefTagID", "48",
                    "RefMsgType", "f",
                    "RefSeqNum", incomeMessage.getField("BeginString")?.getMessage()?.getField("MsgSeqNum"),
                    "Text", "Incorrect instrument",
                    "SessionRejectReason", "99"
            )
            context.send(reject.build())
        }
        else {
            if (incomeMessage.getString("SecurityID") == "INSTR6") {
                val unknownInstr = message("SecurityStatus").addFields(
                        "SecurityID", incomeMessage.getField("SecurityID")!!.getString(),
                        "SecurityIDSource", incomeMessage.getField("SecurityIDSource")!!.getString(),
                        "SecurityStatusReqID", incomeMessage.getField("SecurityStatusReqID")!!.getString(),
                        "UnsolicitedIndicator", "N",
                        "SecurityTradingStatus", "20",
                        "Text", "Unknown or Invalid instrument"
                )
                context.send(unknownInstr.build())
            } else {
                val SecurityStatus1 = message("SecurityStatus").addFields(
                        "SecurityID", incomeMessage.getField("SecurityID")!!.getString(),
                        "SecurityIDSource", incomeMessage.getField("SecurityIDSource")!!.getString(),
                        "SecurityStatusReqID", incomeMessage.getField("SecurityStatusReqID")!!.getString(),
                        "Currency", "RUB",
                        "MarketID", "Demo Market",
                        "MarketSegmentID", "NEW",
                        "TradingSessionID", "1",
                        "TradingSessionSubID", "3",
                        "UnsolicitedIndicator", "N",
                        "SecurityTradingStatus", "17",
                        "BuyVolume", "0",
                        "SellVolume", "0",
                        "HighPx", "56",
                        "LowPx", "54",
                        "LastPx", "54",
                        "FirstPx", "54",
                        "Text", "The simulated SecurityStatus has been sent"
                )
                context.send(SecurityStatus1.build())
            }
        }

        
    }
}
