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

package com.exactpro.th2.sim.template.rule

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.addFields
import com.exactpro.th2.common.message.copy
import com.exactpro.th2.common.message.copyFields
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.message.getMessage
import com.exactpro.th2.common.message.getString
import com.exactpro.th2.common.message.message
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger

class KotlinFIXRule(field: Map<String, Value>) : MessageCompareRule() {

    companion object {
        private var orderId = AtomicInteger(0)
        private var execId = AtomicInteger(0)

        fun reset() {
            orderId.set(0)
            execId.set(0)
        }
    }

    init {
        init("NewOrderSingle", field)
    }

    override fun handle(context: IRuleContext, incomeMessage: Message) {
        val ordId1 = orderId.incrementAndGet()
        val text = incomeMessage.getString("Text")

        if (text == "gen_ER"){
            val fixNew = message("ExecutionReport")
                .copyFields(
                    incomeMessage,
                    "Side",
                    "Price",
                    "CumQty",
                    "ClOrdID",
                    "SecurityID",
                    "SecurityIDSource",
                    "OrdType",
                    "OrderQty",
                    "TradingParty",
                    "TimeInForce",
                    "OrderCapacity",
                    "AccountType"
                ).addFields(
                    "TransactTime", LocalDateTime.now(),
                    "OrderID", ordId1,
                    "LeavesQty", incomeMessage.getField("OrderQty")!!,
                    "Text", "Simulated New Order Buy is placed",
                    "ExecType", "0",
                    "OrdStatus", "0",
                    "CumQty", "0"
                )

            context.send(fixNew.copy().addField("ExecID", execId.incrementAndGet()).build())
            return
        }
        if (text == "gen_BMR"){
            context.send(
                message("BusinessMessageReject").addFields(
                    "RefTagID", "99",
                    "RefMsgType", "D",
                    "RefSeqNum", incomeMessage.getMessage("header")?.getField("MsgSeqNum"),
                    "Text", "StopPx unset for stop order",
                    "BusinessRejectReason", "5",
                    "BusinessRejectRefID", incomeMessage.getField("ClOrdID")
                ).build()
            )
            return
        }
    }
}
