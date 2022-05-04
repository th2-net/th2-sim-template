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
import com.exactpro.th2.common.message.*
import com.exactpro.th2.common.value.getList
import com.exactpro.th2.common.value.getMessage
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule

class KotlinFIXRuleMDR(field: Map<String, Value>) : MessageCompareRule() {

    init {
        init("MarketDataRequest", field)
    }

    override fun handle(context: IRuleContext, incomeMessage: Message) {
        if (incomeMessage.containsFields("MDReqID")) {
            val snapshot = message(
                "MarketDataSnapshotFullRefresh"
            ).copyFields(
                incomeMessage, "MDReqID"
            ).addFields(
                "Instrument", incomeMessage.getField(
                    "NoRelatedSym"
                )!!.getList()!![0].getMessage()!!.getField("Instrument")!!.getMessage()
            ).addFields(
                "NoMDEntries", listOf(
                    message(
                        "SnapShot_NoMDEntries"
                    ).addFields(
                        "MDEntryType",
                        "2",
                        "MDEntrySize",
                        KotlinFIXRule.MDEntry[0].getField("OrderQty"),
                        "MDEntryID",
                        KotlinFIXRule.MDEntry[0].getField("ClOrdID"),
                        "MDEntryPx",
                        KotlinFIXRule.MDEntry[0].getField("Price"),
                        "MDEntryTime",
                        KotlinFIXRule.TradingTime[0],
                        "TradingSessionID",
                        "",
                        "MDEntryBuyer",
                        "DEMO-CONN1",
                        "MDEntrySeller",
                        "DEMOFIRM2",
                        "Text",
                        "Trade data"
                    ), message(
                        "SnapShot_NoMDEntries"
                    ).addFields(
                        "MDEntryType",
                        "2",
                        "MDEntrySize",
                        KotlinFIXRule.MDEntry[1].getField("OrderQty"),
                        "MDEntryID",
                        KotlinFIXRule.MDEntry[1].getField("ClOrdID"),
                        "MDEntryPx",
                        KotlinFIXRule.MDEntry[1].getField("Price"),
                        "MDEntryTime",
                        KotlinFIXRule.TradingTime[1],
                        "TradingSessionID",
                        "",
                        "MDEntryBuyer",
                        "DEMO-CONN1",
                        "MDEntrySeller",
                        "DEMOFIRM2",
                        "Text",
                        "Trade data"
                    )
                )
            )
            context.send(snapshot.build())
        }
    }
}
