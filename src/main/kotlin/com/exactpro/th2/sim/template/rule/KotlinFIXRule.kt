/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.sim.template.FixFields
import java.time.Instant
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

class KotlinFIXRule(fields: Map<String, Any?>, sessionAliases: Map<String, String>) : MessageCompareRule() {

    companion object {
        private const val KEY_ALIAS_1 = "ALIAS_1"
        private const val KEY_ALIAS_2 = "ALIAS_2"
        private const val KEY_DC_ALIAS_1 = "DC_ALIAS_1"
        private const val KEY_DC_ALIAS_2 = "DC_ALIAS_2"

        private val orderId = AtomicInteger(0)
        private val execId = AtomicInteger(0)
        private val TrdMatchId = AtomicInteger(0)

        private val lock = Any()
        private val buyOrdersAndIds: Queue<OrderWithId> = ConcurrentLinkedQueue()
        private val sellOrdersAndIds: Queue<OrderWithId> = ConcurrentLinkedQueue()

        fun reset() {
            orderId.set(0)
            execId.set(0)
            TrdMatchId.set(0)

            synchronized(lock) {
                buyOrdersAndIds.clear()
                sellOrdersAndIds.clear()
            }
        }
    }

    private class OrderWithId(val orderMessage: ParsedMessage, val orderId: Int)

    private val aliases: Map<String, String>

    init {
        init("NewOrderSingle", fields)

        aliases = sessionAliases.toMutableMap().apply {
            putIfAbsent(KEY_ALIAS_1, "fix-demo-server1")
            putIfAbsent(KEY_ALIAS_2, "fix-demo-server2")
            putIfAbsent(KEY_DC_ALIAS_1, "dc-demo-server1")
            putIfAbsent(KEY_DC_ALIAS_2, "dc-demo-server2")
        }
    }

    // FIXME: rule should able to handle several instruments independently
    override fun handle(context: IRuleContext, incomeMessage: ParsedMessage) {
        val now = Instant.now()
        if (!incomeMessage.containsField(FixFields.SIDE)) {
            context.send(
                message("Reject")
                    .addFields(
                        FixFields.REF_TAG_ID to "453",
                        FixFields.REF_MSG_TYPE to "D",
                        FixFields.REF_SEQ_NUM to incomeMessage.getFieldSoft(
                            FixFields.HEADER,
                            FixFields.MSG_SEQ_NUM
                        ),
                        FixFields.TEXT to "Simulating reject message",
                        FixFields.SESSION_REJECT_REASON to "1"
                    ).with(sessionAlias = incomeMessage.id.sessionAlias)
            )
            return
        }

        val instrument = incomeMessage.getField(FixFields.SECURITY_ID)

        if (instrument == "INSTR6") {
            context.send(
                message("BusinessMessageReject")
                    .addFields(
                        FixFields.REF_TAG_ID to "48",
                        FixFields.REF_MSG_TYPE to "D",
                        FixFields.REF_SEQ_NUM to incomeMessage.getFieldSoft("header", FixFields.MSG_SEQ_NUM),
                        FixFields.TEXT to "Unknown SecurityID",
                        FixFields.BUSINESS_REJECT_REASON to "2",
                        FixFields.BUSINESS_REJECT_REF_ID to incomeMessage.getField(FixFields.CL_ORD_ID)
                    ).with(sessionAlias = incomeMessage.id.sessionAlias)
            )
            return
        }

        val incomeOrderId = orderId.incrementAndGet()
        if (incomeMessage.getInt(FixFields.SIDE) == 1) {
            synchronized(lock) {
                buyOrdersAndIds.add(OrderWithId(incomeMessage, incomeOrderId))
            }
            val fixNew = message("ExecutionReport")
                .copyFields(
                    incomeMessage,
                    FixFields.SIDE,
                    FixFields.PRICE,
                    FixFields.CUM_QTY,
                    FixFields.CL_ORD_ID,
                    FixFields.SECONDARY_CL_ORD_ID,
                    FixFields.SECURITY_ID,
                    FixFields.SECURITY_ID_SOURCE,
                    FixFields.ORD_TYPE,
                    FixFields.ORDER_QTY,
                    FixFields.TRADING_PARTY,
                    FixFields.TIME_IN_FORCE,
                    FixFields.ORDER_CAPACITY,
                    FixFields.ACCOUNT_TYPE
                ).addFields(
                    FixFields.TRANSACT_TIME to now,
                    FixFields.ORDER_ID to incomeOrderId,
                    FixFields.LEAVES_QTY to incomeMessage.getField(FixFields.ORDER_QTY)!!,
                    FixFields.TEXT to "Simulated New Order Buy is placed",
                    FixFields.EXEC_TYPE to "0",
                    FixFields.ORD_STATUS to "0",
                    FixFields.CUM_QTY to "0"
                ).build()

            context.send(
                fixNew.toBuilder()
                    .addField(FixFields.EXEC_ID, execId.incrementAndGet())
                    .with(sessionAlias = aliases[KEY_ALIAS_1])
            )
            context.send(
                fixNew.toBuilder()
                    .addField(FixFields.EXEC_ID, execId.incrementAndGet())
                    .with(sessionAlias = aliases[KEY_DC_ALIAS_1])
            )
        } else synchronized(lock) {
            sellOrdersAndIds.add(OrderWithId(incomeMessage, incomeOrderId))
        }

        val sellOrder: OrderWithId
        val firstBuyOrder: OrderWithId
        val secondBuyOrder: OrderWithId

        synchronized(lock) {
            if (buyOrdersAndIds.size < 2 || sellOrdersAndIds.isEmpty()) {
                // we don't have enough orders for matching
                return
            }

            // Useful variables for buy-side
            sellOrder = sellOrdersAndIds.remove()
            firstBuyOrder = buyOrdersAndIds.remove()
            secondBuyOrder = buyOrdersAndIds.remove()
        }

        val cumQty1 = secondBuyOrder.orderMessage.getInt(FixFields.ORDER_QTY)!!
        val cumQty2 = firstBuyOrder.orderMessage.getInt(FixFields.ORDER_QTY)!!
        val leavesQty2 = sellOrder.orderMessage.getInt(FixFields.ORDER_QTY)!! - (cumQty1 + cumQty2)
        val order1Price = firstBuyOrder.orderMessage.getString(FixFields.PRICE)!!
        val order2Price = secondBuyOrder.orderMessage.getString(FixFields.PRICE)!!

        val tradeMatchId1 = TrdMatchId.incrementAndGet()
        val tradeMatchId2 = TrdMatchId.incrementAndGet()

        // Generator ER
        // ER FF Order2 for Trader1
        val noPartyIdsTrader2Order3 = hashMapOf(
            FixFields.NO_PARTY_IDS to createNoPartyIds("DEMO-CONN2", "DEMOFIRM1")
        )

        val trader1 = message("ExecutionReport")
            .copyFields(
                sellOrder.orderMessage,
                FixFields.SECURITY_ID,
                FixFields.SECURITY_ID_SOURCE,
                FixFields.ORD_TYPE,
                FixFields.ORDER_CAPACITY,
                FixFields.ACCOUNT_TYPE
            ).addFields(
                FixFields.TRADING_PARTY to hashMapOf(
                    FixFields.NO_PARTY_IDS to createNoPartyIds("DEMO-CONN1", "DEMOFIRM2")
                ),
                FixFields.SIDE to "1",
                FixFields.TIME_IN_FORCE to "0",  // Get from message?
                FixFields.EXEC_TYPE to "F",
                FixFields.ORD_STATUS to "2",
                FixFields.LEAVES_QTY to 0,
                FixFields.TEXT to "The simulated order has been fully traded"
            ).build()

        val trader1Order2 = trader1.toBuilder()
            .copyFields(
                secondBuyOrder.orderMessage,
                FixFields.CL_ORD_ID,
                FixFields.SECONDARY_CL_ORD_ID,
                FixFields.ORDER_QTY
            )
            .addFields(
                FixFields.TRANSACT_TIME to now,
                FixFields.CUM_QTY to cumQty1,
                FixFields.PRICE to order2Price,
                FixFields.LAST_PX to order2Price,
                FixFields.ORDER_ID to secondBuyOrder.orderId,
                FixFields.EXEC_ID to execId.incrementAndGet(),
                FixFields.TRD_MATCH_ID to tradeMatchId1
            ).build()

        context.send(trader1Order2.toBuilder().with(sessionAlias = aliases[KEY_ALIAS_1]))
        context.send(trader1Order2.toBuilder().with(sessionAlias = aliases[KEY_DC_ALIAS_1]))

        // ER FF Order1 for Trader1
        val trader1Order1 = trader1.toBuilder()
            .copyFields(
                firstBuyOrder.orderMessage,
                FixFields.CL_ORD_ID,
                FixFields.SECONDARY_CL_ORD_ID,
                FixFields.ORDER_QTY,
                FixFields.PRICE
            )
            .addFields(
                FixFields.TRANSACT_TIME to now,
                FixFields.CUM_QTY to cumQty2,
                FixFields.LAST_PX to firstBuyOrder.orderMessage.getField(FixFields.PRICE),
                FixFields.ORDER_ID to firstBuyOrder.orderId,
                FixFields.EXEC_ID to execId.incrementAndGet(),
                FixFields.TRD_MATCH_ID to tradeMatchId2,
            ).build()

        context.send(trader1Order1.toBuilder().with(sessionAlias = aliases[KEY_ALIAS_1]))
        context.send(trader1Order1.toBuilder().with(sessionAlias = aliases[KEY_DC_ALIAS_1]))

        val trader2 = message("ExecutionReport").copyFields(
            sellOrder.orderMessage,
            FixFields.TIME_IN_FORCE,
            FixFields.SIDE,
            FixFields.PRICE,
            FixFields.CL_ORD_ID,
            FixFields.SECONDARY_CL_ORD_ID,
            FixFields.SECURITY_ID,
            FixFields.SECURITY_ID_SOURCE,
            FixFields.ORD_TYPE,
            FixFields.ORDER_CAPACITY,
            FixFields.ACCOUNT_TYPE
        ).addFields(
            FixFields.ORDER_ID to sellOrder.orderId
        ).build()

        val trader2Order3 = trader2.toBuilder()
            .addFields(
                FixFields.TRADING_PARTY to noPartyIdsTrader2Order3,
                FixFields.EXEC_TYPE to "F",
                FixFields.ORD_STATUS to "1",
                FixFields.ORDER_QTY to sellOrder.orderMessage.getString(FixFields.ORDER_QTY)!!,
                FixFields.TEXT to "The simulated order has been partially traded"
            ).build()

        val trader2Order3Er1 = trader2Order3.toBuilder()
            .addFields(
                FixFields.TRANSACT_TIME to now,
                FixFields.LAST_PX to order2Price,
                FixFields.CUM_QTY to cumQty1,
                FixFields.LEAVES_QTY to sellOrder.orderMessage.getInt(FixFields.ORDER_QTY)!! - cumQty1,
                FixFields.EXEC_ID to execId.incrementAndGet(),
                FixFields.TRD_MATCH_ID to tradeMatchId1,
            ).build()

        // ER1 PF Order3 for Trader2
        context.send(trader2Order3Er1.toBuilder().with(sessionAlias = aliases[KEY_ALIAS_2]))
        //DropCopy
        context.send(trader2Order3Er1.toBuilder().with(sessionAlias = aliases[KEY_DC_ALIAS_2]))

        // ER2 PF Order3 for Trader2
        val trader2Order3Er2 = trader2Order3.toBuilder().addFields(
            FixFields.TRANSACT_TIME to now,
            FixFields.LAST_PX to order1Price,
            FixFields.CUM_QTY to cumQty1 + cumQty2,
            FixFields.LEAVES_QTY to leavesQty2,
            FixFields.EXEC_ID to execId.incrementAndGet(),
            FixFields.TRD_MATCH_ID to tradeMatchId2,
        ).build()

        context.send(trader2Order3Er2.toBuilder().apply {
            with(sessionAlias = aliases[KEY_ALIAS_2])
            if (instrument == "INSTR5") {
                addFields(
                    FixFields.TEXT to "Execution Report with incorrect value in OrdStatus tag",
                    FixFields.ORDER_CAPACITY to "P",  // Incorrect value as testcase
                    FixFields.ACCOUNT_TYPE to "2"     // Incorrect value as testcase
                )
            }
        })

        //DropCopy
        context.send(trader2Order3Er2.toBuilder().with(sessionAlias = aliases[KEY_DC_ALIAS_2]))

        if (instrument == "INSTR4") {
            // Extra ER3 FF Order3 for Trader2 as testcase
            val trader2Order3fixX = trader2.toBuilder()
                .addFields(
                    FixFields.TRANSACT_TIME to now,
                    FixFields.TRADING_PARTY to noPartyIdsTrader2Order3,
                    FixFields.EXEC_TYPE to "F",
                    FixFields.ORD_STATUS to "2",
                    FixFields.LAST_PX to order1Price,
                    FixFields.CUM_QTY to cumQty1 + cumQty2,
                    FixFields.ORDER_QTY to sellOrder.orderMessage.getString(FixFields.ORDER_QTY)!!,
                    FixFields.LEAVES_QTY to leavesQty2,
                    FixFields.EXEC_ID to execId.incrementAndGet(),
                    FixFields.TRD_MATCH_ID to tradeMatchId2,
                    FixFields.TEXT to "Extra Execution Report"
                )
            context.send(trader2Order3fixX.with(sessionAlias = aliases[KEY_ALIAS_2]))
        }

        // ER3 CC Order3 for Trader2
        val trader2Order3Er3CC = trader2.toBuilder()
            .copyFields(sellOrder.orderMessage, FixFields.TRADING_PARTY)
            .addFields(
                FixFields.TRANSACT_TIME to now,
                FixFields.EXEC_TYPE to "C",
                FixFields.ORD_STATUS to "C",
                FixFields.CUM_QTY to cumQty1 + cumQty2,
                FixFields.LEAVES_QTY to "0",
                FixFields.ORDER_QTY to sellOrder.orderMessage.getString(FixFields.ORDER_QTY)!!,
                FixFields.EXEC_ID to execId.incrementAndGet(),
                FixFields.TEXT to "The remaining part of simulated order has been expired"
            ).build()
        context.send(trader2Order3Er3CC.toBuilder().with(sessionAlias = aliases[KEY_ALIAS_2]))
        //DropCopy
        context.send(trader2Order3Er3CC.toBuilder().with(sessionAlias = aliases[KEY_DC_ALIAS_2]))
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
            FixFields.PARTY_ROLE to partyRole,
            FixFields.PARTY_ID to partyID,
            FixFields.PARTY_ID_SOURCE to partyIDSource,
        )

    private fun ParsedMessage.FromMapBuilder.with(sessionAlias: String? = null): ParsedMessage.FromMapBuilder = apply {
        with(idBuilder()) {
            sessionAlias?.let(this::setSessionAlias)
        }
    }
}