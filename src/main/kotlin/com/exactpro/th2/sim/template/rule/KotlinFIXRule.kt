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
import com.exactpro.th2.common.message.copyField
import com.exactpro.th2.common.message.copyFields
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.message.getInt
import com.exactpro.th2.common.message.getMessage
import com.exactpro.th2.common.message.getString
import com.exactpro.th2.common.message.hasField
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.value.getMessage
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger

class KotlinFIXRule(field: Map<String, Value>) : MessageCompareRule() {
    private val alias1 = "fix-demo-server1";
    private val alias2 = "fix-demo-server2";
    private val aliasdc1 = "dc-demo-server1";
    private val aliasdc2 = "dc-demo-server2";

    companion object {
        private var orderId = AtomicInteger(0)
        private var execId = AtomicInteger(0)
        private var TrdMatchId = AtomicInteger(0)

        private var incomeMsgList = arrayListOf<Message>()
        private var ordIdList = arrayListOf<Int>()

        fun reset() {
            orderId.set(0)
            execId.set(0)
            TrdMatchId.set(0)

            incomeMsgList.clear()
            ordIdList.clear()
        }
    }

    init {
        init("NewOrderSingle", field)
    }

    override fun handle(context: IRuleContext, incomeMessage: Message) {
        incomeMsgList.add(incomeMessage)
        while (incomeMsgList.size > 3) {
            incomeMsgList.removeAt(0)
        }
        val ordId1 = orderId.incrementAndGet()

        ordIdList.add(ordId1)
        while (ordIdList.size > 3) {
            ordIdList.removeAt(0)
        }

        if (!incomeMessage.hasField("Side")) {
            context.send(
                message("Reject").addFields(
                    "RefTagID", "453",
                    "RefMsgType", "D",
                    "RefSeqNum", incomeMessage.getField("BeginString")?.getMessage()?.getField("MsgSeqNum"),
                    "Text", "Simulating reject message",
                    "SessionRejectReason", "1"
                ).build()
            )
            return
        }

        val instrument = incomeMessage.getString("SecurityID")

        if (instrument == "INSTR6") {
            context.send(
                message("BusinessMessageReject").addFields(
                    "RefTagID", "48",
                    "RefMsgType", "D",
                    "RefSeqNum", incomeMessage.getMessage("header")?.getField("MsgSeqNum"),
                    "Text", "Unknown SecurityID",
                    "BusinessRejectReason", "2",
                    "BusinessRejectRefID", incomeMessage.getField("ClOrdID")
                ).build()
            )
            return
        }

        if (incomeMessage.getInt("Side") == 1) {
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

            context.send(fixNew.copy().addField("ExecID", execId.incrementAndGet())
                .buildWith { sessionAlias = alias1 })
            context.send(fixNew.copy().addField("ExecID", execId.incrementAndGet())
                .buildWith { sessionAlias = aliasdc1 })

            return
        } else {
            // Useful variables for buy-side
            val first = incomeMsgList[0]
            val second = incomeMsgList[1]

            val cumQty1 = second.getInt("OrderQty")!!
            val cumQty2 = first.getInt("OrderQty")!!
            val leavesQty2 = incomeMessage.getInt("OrderQty")!! - (cumQty1 + cumQty2)
            val order1Price = first.getString("Price")!!
            val order2Price = second.getString("Price")!!

            val tradeMatchID1 = TrdMatchId.incrementAndGet()
            val tradeMatchID2 = TrdMatchId.incrementAndGet()

            // Generator ER
            // ER FF Order2 for Trader1
            val transTime1 = LocalDateTime.now()
            val transTime2 = LocalDateTime.now()

            val noPartyIdsTrader2Order3 = message().addFields(
                "NoPartyIDs", createNoPartyIdsList("DEMO-CONN2", "DEMOFIRM1")
            )

            val trader1 = message("ExecutionReport")
                .copyFields(
                    incomeMessage,
                    "SecurityID",
                    "SecurityIDSource",
                    "OrdType",
                    "OrderCapacity",
                    "AccountType"
                )
                .addFields(
                    "TradingParty", message().addField(
                        "NoPartyIDs", createNoPartyIdsList("DEMO-CONN1", "DEMOFIRM2")
                    ),
                    "Side", 1,
                    "TimeInForce", 0,  // Get from message?
                    "ExecType", "F",
                    "OrdStatus", 2,
                    "LeavesQty", 0,
                    "Text", "The simulated order has been fully traded"
                )

            val trader1Order2 = trader1.copy()
                .copyFields(second, "ClOrdID", "OrderQty")
                .addFields(
                    "TransactTime", transTime1,
                    "CumQty", cumQty1,
                    "Price", order2Price,
                    "LastPx", order2Price,
                    "OrderID", ordIdList[1],
                    "ExecID", execId.incrementAndGet(),
                    "TrdMatchID", tradeMatchID1
                )

            context.send(trader1Order2.copy().buildWith { sessionAlias = alias1 })
            context.send(trader1Order2.copy().buildWith { sessionAlias = aliasdc1 })

            // ER FF Order1 for Trader1
            val trader1Order1 = trader1.copy()
                .copyFields(first, "ClOrdID", "OrderQty", "Price")
                .addFields(
                    "TransactTime", transTime2,
                    "CumQty", cumQty2,
                    "LastPx", first.getField("Price"),
                    "OrderID", ordIdList[0],
                    "ExecID", execId.incrementAndGet(),
                    "TrdMatchID", tradeMatchID2,
                )

            context.send(trader1Order1.copy().buildWith { sessionAlias = alias1 })
            context.send(trader1Order1.copy().buildWith { sessionAlias = aliasdc1 })

            val trader2 = message("ExecutionReport").copyFields(
                incomeMessage,
                "TimeInForce",
                "Side",
                "Price",
                "ClOrdID",
                "SecurityID",
                "SecurityIDSource",
                "OrdType",
                "OrderCapacity",
                "AccountType"
            ).addFields(
                "OrderID", ordId1
            )

            val trader2Order3 = trader2.copy().addFields(
                "TradingParty", noPartyIdsTrader2Order3,
                "ExecType", "F",
                "OrdStatus", "1",
                "OrderQty", incomeMessage.getString("OrderQty")!!,
                "Text", "The simulated order has been partially traded"
            )

            val trader2Order3Er1 = trader2Order3.copy().addFields(
                "TransactTime", transTime1,
                "LastPx", order2Price,
                "CumQty", cumQty1,
                "LeavesQty", incomeMessage.getInt("OrderQty")!! - cumQty1,
                "ExecID", execId.incrementAndGet(),
                "TrdMatchID", tradeMatchID1,
            )

            // ER1 PF Order3 for Trader2
            context.send(trader2Order3Er1.copy().buildWith { sessionAlias = alias2 })
            //DropCopy
            context.send(trader2Order3Er1.copy().buildWith { sessionAlias = aliasdc2 })

            // ER2 PF Order3 for Trader2
            val trader2Order3Er2 = trader2Order3.copy().addFields(
                "TransactTime", transTime2,
                "LastPx", order1Price,
                "CumQty", cumQty1 + cumQty2,
                "LeavesQty", leavesQty2,
                "ExecID", execId.incrementAndGet(),
                "TrdMatchID", tradeMatchID2,
            )

            context.send(trader2Order3Er2.copy().buildWith {
                sessionAlias = alias2
                if (instrument == "INSTR5") {
                    addFields(
                        "Text", "Execution Report with incorrect value in OrdStatus tag",
                        "OrderCapacity", "P",  // Incorrect value as testcase
                        "AccountType", "2"     // Incorrect value as testcase
                    )
                }
            })
            //DropCopy
            context.send(trader2Order3Er2.copy().buildWith { sessionAlias = aliasdc2 })

            if (instrument == "INSTR4") {
                // Extra ER3 FF Order3 for Trader2 as testcase
                val trader2Order3fixX = trader2.copy()
                    .addFields(
                        "TransactTime", transTime2,
                        "TradingParty", noPartyIdsTrader2Order3,
                        "ExecType", "F",
                        "OrdStatus", "2",
                        "LastPx", order1Price,
                        "CumQty", cumQty1 + cumQty2,
                        "OrderQty", incomeMessage.getString("OrderQty")!!,
                        "LeavesQty", leavesQty2,
                        "ExecID", execId.incrementAndGet(),
                        "TrdMatchID", tradeMatchID2,
                        "Text", "Extra Execution Report"
                    )
                context.send(trader2Order3fixX.buildWith { sessionAlias = alias2 })
            }
            // ER3 CC Order3 for Trader2
            val trader2Order3Er3CC = trader2.copy()
                .copyField(incomeMessage, "TradingParty")
                .addFields(
                    "TransactTime", LocalDateTime.now(),
                    "ExecType", "C",
                    "OrdStatus", "C",
                    "CumQty", cumQty1 + cumQty2,
                    "LeavesQty", "0",
                    "OrderQty", incomeMessage.getString("OrderQty")!!,
                    "ExecID", execId.incrementAndGet(),
                    "Text", "The remaining part of simulated order has been expired"
                )
            context.send(trader2Order3Er3CC.copy().buildWith { sessionAlias = alias2 })
            //DropCopy
            context.send(trader2Order3Er3CC.copy().buildWith { sessionAlias = aliasdc2 })
        }
    }

    private fun createNoPartyIdsList(connect: String, firm: String): List<Message.Builder> = listOf(
        createPartyMessage("76", connect, "D"),
        createPartyMessage("17", firm, "D"),
        createPartyMessage("3", "0", "P"),
        createPartyMessage("122", "0", "P"),
        createPartyMessage("12", "3", "P")
    )

    private fun createPartyMessage(partyRole: String, partyID: String, partyIDSource: String): Message.Builder =
        message().addFields(
            "PartyRole", partyRole,
            "PartyID", partyID,
            "PartyIDSource", partyIDSource
        )

    private fun Message.Builder.buildWith(action: Message.Builder.() -> Unit): Message {
        action(this)
        return this.build()
    }
}
