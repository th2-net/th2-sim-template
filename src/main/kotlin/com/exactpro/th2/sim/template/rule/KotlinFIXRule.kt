/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.utils.message.transport.copyFields
import com.exactpro.th2.common.utils.message.transport.getField
import com.exactpro.th2.common.utils.message.transport.getFieldSoft
import com.exactpro.th2.common.utils.message.transport.getInt
import com.exactpro.th2.common.utils.message.transport.getString
import com.exactpro.th2.common.utils.message.transport.message
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule
import java.time.LocalDateTime
import java.util.Queue
import java.util.LinkedList
import java.util.concurrent.atomic.AtomicInteger

class KotlinFIXRule(field: Map<String, Any?>) : MessageCompareRule() {
    private val alias1 = "fix-demo-server1"
    private val alias2 = "fix-demo-server2"
    private val aliasdc1 = "dc-demo-server1"
    private val aliasdc2 = "dc-demo-server2"

    companion object {
        private val orderId = AtomicInteger(0)
        private val execId = AtomicInteger(0)
        private val TrdMatchId = AtomicInteger(0)

        private val buyOrdersAndIds: Queue<Pair<ParsedMessage, Int>> = LinkedList()
        private val sellOrders: Queue<Pair<ParsedMessage, Int>> = LinkedList()

        fun reset() {
            orderId.set(0)
            execId.set(0)
            TrdMatchId.set(0)

            buyOrdersAndIds.clear()
            sellOrders.clear()
        }
    }

    init {
        init("NewOrderSingle", field)
    }

    override fun handle(context: IRuleContext, incomeMessage: ParsedMessage) {
        if (!incomeMessage.containsField("Side")) {
            context.send(
                message("Reject")
                    .addFields(
                        "RefTagID" to "453",
                        "RefMsgType" to "D",
                        "RefSeqNum" to incomeMessage.getFieldSoft("BeginString", "MsgSeqNum"),
                        "Text" to "Simulating reject message",
                        "SessionRejectReason" to "1"
                    ).with(sessionAlias = incomeMessage.id.sessionAlias)
            )
            return
        }

        val instrument = incomeMessage.getField("SecurityID")

        if (instrument == "INSTR6") {
            context.send(
                message("BusinessMessageReject")
                    .addFields(
                        "RefTagID" to "48",
                        "RefMsgType" to "D",
                        "RefSeqNum" to incomeMessage.getFieldSoft("header", "MsgSeqNum"),
                        "Text" to "Unknown SecurityID",
                        "BusinessRejectReason" to "2",
                        "BusinessRejectRefID" to incomeMessage.getField("ClOrdID")
                    ).with(sessionAlias = incomeMessage.id.sessionAlias)
            )
            return
        }

        val incomeOrderId = orderId.incrementAndGet()
        if (incomeMessage.getInt("Side") == 1) {
            buyOrdersAndIds.add(incomeMessage to incomeOrderId)
            val fixNew = message("ExecutionReport")
                .copyFields(
                    incomeMessage,
                    "Side",
                    "Price",
                    "CumQty",
                    "ClOrdID",
                    "SecondaryClOrdID",
                    "SecurityID",
                    "SecurityIDSource",
                    "OrdType",
                    "OrderQty",
                    "TradingParty",
                    "TimeInForce",
                    "OrderCapacity",
                    "AccountType"
                ).addFields(
                    "TransactTime" to LocalDateTime.now(),
                    "OrderID" to incomeOrderId,
                    "LeavesQty" to incomeMessage.getField("OrderQty")!!,
                    "Text" to "Simulated New Order Buy is placed",
                    "ExecType" to "0",
                    "OrdStatus" to "0",
                    "CumQty" to "0"
                ).build()

            context.send(
                fixNew.toBuilder()
                    .addField("ExecID", execId.incrementAndGet())
                    .with(sessionAlias = alias1)
            )
            context.send(
                fixNew.toBuilder()
                    .addField("ExecID", execId.incrementAndGet())
                    .with(sessionAlias = aliasdc1)
            )
        } else {
            sellOrders.add(incomeMessage to incomeOrderId)
        }

        if (buyOrdersAndIds.size < 2 || sellOrders.isEmpty()) {
            // we don't have enough orders for matching
            return
        }

        // Useful variables for buy-side
        val (sellOrder, sellId) = sellOrders.remove()
        val (firstBuyOrder, firstId) = buyOrdersAndIds.remove()
        val (secondBuyOrder, secondId) = buyOrdersAndIds.remove()

        val cumQty1 = secondBuyOrder.getInt("OrderQty")!!
        val cumQty2 = firstBuyOrder.getInt("OrderQty")!!
        val leavesQty2 = sellOrder.getInt("OrderQty")!! - (cumQty1 + cumQty2)
        val order1Price = firstBuyOrder.getString("Price")!!
        val order2Price = secondBuyOrder.getString("Price")!!

        val tradeMatchID1 = TrdMatchId.incrementAndGet()
        val tradeMatchID2 = TrdMatchId.incrementAndGet()

        // Generator ER
        // ER FF Order2 for Trader1
        val transTime1 = LocalDateTime.now()
        val transTime2 = LocalDateTime.now()

        val noPartyIdsTrader2Order3 = hashMapOf(
            "NoPartyIDs" to createNoPartyIds("DEMO-CONN2", "DEMOFIRM1")
        )

        val trader1 = message("ExecutionReport")
            .copyFields(
                sellOrder,
                "SecurityID",
                "SecurityIDSource",
                "OrdType",
                "OrderCapacity",
                "AccountType"
            ).addFields(
                "TradingParty" to hashMapOf(
                    "NoPartyIDs" to createNoPartyIds("DEMO-CONN1", "DEMOFIRM2")
                ),
                "Side" to "1",
                "TimeInForce" to "0",  // Get from message?
                "ExecType" to "F",
                "OrdStatus" to "2",
                "LeavesQty" to 0,
                "Text" to "The simulated order has been fully traded"
            ).build()

        val trader1Order2 = trader1.toBuilder()
            .copyFields(secondBuyOrder, "ClOrdID", "SecondaryClOrdID", "OrderQty")
            .addFields(
                "TransactTime" to transTime1,
                "CumQty" to cumQty1,
                "Price" to order2Price,
                "LastPx" to order2Price,
                "OrderID" to secondId,
                "ExecID" to execId.incrementAndGet(),
                "TrdMatchID" to tradeMatchID1
            ).build()

        context.send(trader1Order2.toBuilder().with(sessionAlias = alias1))
        context.send(trader1Order2.toBuilder().with(sessionAlias = aliasdc1))

        // ER FF Order1 for Trader1
        val trader1Order1 = trader1.toBuilder()
            .copyFields(firstBuyOrder, "ClOrdID", "SecondaryClOrdID", "OrderQty", "Price")
            .addFields(
                "TransactTime" to transTime2,
                "CumQty" to cumQty2,
                "LastPx" to firstBuyOrder.getField("Price"),
                "OrderID" to firstId,
                "ExecID" to execId.incrementAndGet(),
                "TrdMatchID" to tradeMatchID2,
            ).build()

        context.send(trader1Order1.toBuilder().with(sessionAlias = alias1))
        context.send(trader1Order1.toBuilder().with(sessionAlias = aliasdc1))

        val trader2 = message("ExecutionReport").copyFields(
            sellOrder,
            "TimeInForce",
            "Side",
            "Price",
            "ClOrdID",
            "SecondaryClOrdID",
            "SecurityID",
            "SecurityIDSource",
            "OrdType",
            "OrderCapacity",
            "AccountType"
        ).addFields(
            "OrderID" to sellId
        ).build()

        val trader2Order3 = trader2.toBuilder()
            .addFields(
                "TradingParty" to noPartyIdsTrader2Order3,
                "ExecType" to "F",
                "OrdStatus" to "1",
                "OrderQty" to sellOrder.getString("OrderQty")!!,
                "Text" to "The simulated order has been partially traded"
            ).build()

        val trader2Order3Er1 = trader2Order3.toBuilder()
            .addFields(
                "TransactTime" to transTime1,
                "LastPx" to order2Price,
                "CumQty" to cumQty1,
                "LeavesQty" to sellOrder.getInt("OrderQty")!! - cumQty1,
                "ExecID" to execId.incrementAndGet(),
                "TrdMatchID" to tradeMatchID1,
            ).build()

        // ER1 PF Order3 for Trader2
        context.send(trader2Order3Er1.toBuilder().with(sessionAlias = alias2))
        //DropCopy
        context.send(trader2Order3Er1.toBuilder().with(sessionAlias = aliasdc2))

        // ER2 PF Order3 for Trader2
        val trader2Order3Er2 = trader2Order3.toBuilder().addFields(
            "TransactTime" to transTime2,
            "LastPx" to order1Price,
            "CumQty" to cumQty1 + cumQty2,
            "LeavesQty" to leavesQty2,
            "ExecID" to execId.incrementAndGet(),
            "TrdMatchID" to tradeMatchID2,
        ).build()

        context.send(trader2Order3Er2.toBuilder().apply {
            with(sessionAlias = alias2)
            if (instrument == "INSTR5") {
                addFields(
                    "Text" to "Execution Report with incorrect value in OrdStatus tag",
                    "OrderCapacity" to "P",  // Incorrect value as testcase
                    "AccountType" to "2"     // Incorrect value as testcase
                )
            }
        })

        //DropCopy
        context.send(trader2Order3Er2.toBuilder().with(sessionAlias = aliasdc2))

        if (instrument == "INSTR4") {
            // Extra ER3 FF Order3 for Trader2 as testcase
            val trader2Order3fixX = trader2.toBuilder()
                .addFields(
                    "TransactTime" to transTime2,
                    "TradingParty" to noPartyIdsTrader2Order3,
                    "ExecType" to "F",
                    "OrdStatus" to "2",
                    "LastPx" to order1Price,
                    "CumQty" to cumQty1 + cumQty2,
                    "OrderQty" to sellOrder.getString("OrderQty")!!,
                    "LeavesQty" to leavesQty2,
                    "ExecID" to execId.incrementAndGet(),
                    "TrdMatchID" to tradeMatchID2,
                    "Text" to "Extra Execution Report"
                )
            context.send(trader2Order3fixX.with(sessionAlias = alias2))
        }

        // ER3 CC Order3 for Trader2
        val trader2Order3Er3CC = trader2.toBuilder()
            .copyFields(sellOrder, "TradingParty")
            .addFields(
                "TransactTime" to LocalDateTime.now(),
                "ExecType" to "C",
                "OrdStatus" to "C",
                "CumQty" to cumQty1 + cumQty2,
                "LeavesQty" to "0",
                "OrderQty" to sellOrder.getString("OrderQty")!!,
                "ExecID" to execId.incrementAndGet(),
                "Text" to "The remaining part of simulated order has been expired"
            ).build()
        context.send(trader2Order3Er3CC.toBuilder().with(sessionAlias = alias2))
        //DropCopy
        context.send(trader2Order3Er3CC.toBuilder().with(sessionAlias = aliasdc2))
    }

    private fun createNoPartyIds(connect: String, firm: String): List<Map<String, Any>> = listOf(
        createParty("76", connect, "D"),
        createParty("17", firm, "D"),
        createParty("3", "0", "P"),
        createParty("122", "0", "P"),
        createParty("12", "3", "P")
    )

    private fun createParty(partyRole: String, partyID: String, partyIDSource: String): Map<String, Any> =
        hashMapOf(
            "PartyRole" to partyRole,
            "PartyID" to partyID,
            "PartyIDSource" to partyIDSource,
        )

    private fun ParsedMessage.FromMapBuilder.with(sessionAlias: String? = null): ParsedMessage.FromMapBuilder = apply {
        with(idBuilder()) {
            sessionAlias?.let(this::setSessionAlias)
        }
    }
}