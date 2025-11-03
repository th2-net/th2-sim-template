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
import com.exactpro.th2.sim.template.FixFields.Companion.ACCOUNT_TYPE
import com.exactpro.th2.sim.template.FixFields.Companion.AGGRESSOR_INDICATOR
import com.exactpro.th2.sim.template.FixFields.Companion.BUSINESS_REJECT_REASON
import com.exactpro.th2.sim.template.FixFields.Companion.BUSINESS_REJECT_REF_ID
import com.exactpro.th2.sim.template.FixFields.Companion.CL_ORD_ID
import com.exactpro.th2.sim.template.FixFields.Companion.CUM_QTY
import com.exactpro.th2.sim.template.FixFields.Companion.EXEC_ID
import com.exactpro.th2.sim.template.FixFields.Companion.EXEC_TYPE
import com.exactpro.th2.sim.template.FixFields.Companion.HEADER
import com.exactpro.th2.sim.template.FixFields.Companion.LAST_PX
import com.exactpro.th2.sim.template.FixFields.Companion.LEAVES_QTY
import com.exactpro.th2.sim.template.FixFields.Companion.MSG_SEQ_NUM
import com.exactpro.th2.sim.template.FixFields.Companion.MSG_TYPE
import com.exactpro.th2.sim.template.FixFields.Companion.NO_PARTY_IDS
import com.exactpro.th2.sim.template.FixFields.Companion.ORDER_CAPACITY
import com.exactpro.th2.sim.template.FixFields.Companion.ORDER_ID
import com.exactpro.th2.sim.template.FixFields.Companion.ORDER_QTY
import com.exactpro.th2.sim.template.FixFields.Companion.ORD_STATUS
import com.exactpro.th2.sim.template.FixFields.Companion.ORD_TYPE
import com.exactpro.th2.sim.template.FixFields.Companion.PARTY_ID
import com.exactpro.th2.sim.template.FixFields.Companion.PARTY_ID_SOURCE
import com.exactpro.th2.sim.template.FixFields.Companion.PARTY_ROLE
import com.exactpro.th2.sim.template.FixFields.Companion.PRICE
import com.exactpro.th2.sim.template.FixFields.Companion.REF_MSG_TYPE
import com.exactpro.th2.sim.template.FixFields.Companion.REF_SEQ_NUM
import com.exactpro.th2.sim.template.FixFields.Companion.REF_TAG_ID
import com.exactpro.th2.sim.template.FixFields.Companion.SECONDARY_CL_ORD_ID
import com.exactpro.th2.sim.template.FixFields.Companion.SECURITY_ID
import com.exactpro.th2.sim.template.FixFields.Companion.SECURITY_ID_SOURCE
import com.exactpro.th2.sim.template.FixFields.Companion.SESSION_REJECT_REASON
import com.exactpro.th2.sim.template.FixFields.Companion.SIDE
import com.exactpro.th2.sim.template.FixFields.Companion.TEXT
import com.exactpro.th2.sim.template.FixFields.Companion.TIME_IN_FORCE
import com.exactpro.th2.sim.template.FixFields.Companion.TRADING_PARTY
import com.exactpro.th2.sim.template.FixFields.Companion.TRANSACT_TIME
import com.exactpro.th2.sim.template.FixFields.Companion.TRD_MATCH_ID
import com.exactpro.th2.sim.template.FixValues.Companion.BUSINESS_REJECT_REASON_UNKNOWN_SECURITY
import com.exactpro.th2.sim.template.FixValues.Companion.SESSION_REJECT_REASON_REQUIRED_TAG_MISSING
import com.exactpro.th2.sim.template.FixValues.Companion.SIDE_BUY
import com.exactpro.th2.sim.template.rule.Action.ADD
import com.exactpro.th2.sim.template.rule.Action.DELETE
import com.opencsv.CSVWriter
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.LinkedList
import java.util.Queue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract
import kotlin.io.path.createDirectories
import kotlin.io.path.extension
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.io.path.notExists

private val LOGGER = KotlinLogging.logger {}

class KotlinFIXRule(fields: Map<String, Any?>, sessionAliases: Map<String, String>) : MessageCompareRule() {

    companion object {
        private const val ENV_BOOK_LOG_DIR = "th2.sim.kotlin-fix-rule.book-log.dir"
        private const val ENV_BOOK_LOG_FILE_PATTERN = "th2.sim.kotlin-fix-rule.book-log.pattern"

        private const val KEY_ALIAS_1 = "ALIAS_1"
        private const val KEY_ALIAS_2 = "ALIAS_2"
        private const val KEY_DC_ALIAS_1 = "DC_ALIAS_1"
        private const val KEY_DC_ALIAS_2 = "DC_ALIAS_2"

        private val orderId = AtomicInteger(0)
        private val execId = AtomicInteger(0)
        private val matchId = AtomicInteger(0)

        private val books = ConcurrentHashMap<String, Book>()

        fun reset() {
            orderId.set(0)
            execId.set(0)
            matchId.set(0)

            books.clear()
        }
    }

    private val aliases: Map<String, String>
    private val bookLog: BookLog?

    init {
        init("NewOrderSingle", fields)

        aliases = sessionAliases.toMutableMap().apply {
            putIfAbsent(KEY_ALIAS_1, "fix-demo-server1")
            putIfAbsent(KEY_ALIAS_2, "fix-demo-server2")
            putIfAbsent(KEY_DC_ALIAS_1, "dc-demo-server1")
            putIfAbsent(KEY_DC_ALIAS_2, "dc-demo-server2")
        }

        //FIXME: several rules can affect each other
        bookLog = CsvBookLog.createCsvBookLog(
            System.getProperty(ENV_BOOK_LOG_DIR)?.run(Path::of),
            System.getProperty(ENV_BOOK_LOG_FILE_PATTERN)
        )
    }

    override fun handle(context: IRuleContext, message: ParsedMessage) {
        val handleTimestamp = Instant.now()
        if (!message.containsField(SIDE)) {
            sendReject(context, message, "453")
            return
        }

        val instrument = message.getString(SECURITY_ID)
        if (instrument == null) {
            sendReject(context, message, "48")
            return
        }

        if (instrument == "INSTR6") {
            sendBusinessMessageReject(context, message, "48")
            return
        }

        val incomeOrderId = orderId.incrementAndGet()
        val book = books.computeIfAbsent(instrument) { Book(bookLog) }
        if (message.getString(SIDE) == SIDE_BUY) {
            book.addBuy(incomeOrderId, handleTimestamp, message)
            val fixNew = message("ExecutionReport")
                .copyFields(
                    message,
                    ACCOUNT_TYPE, CL_ORD_ID, CUM_QTY, ORDER_CAPACITY, ORDER_QTY, ORD_TYPE, PRICE, SECONDARY_CL_ORD_ID,
                    SECURITY_ID, SECURITY_ID_SOURCE, SIDE, TIME_IN_FORCE, TRADING_PARTY
                ).addFields(
                    TRANSACT_TIME to handleTimestamp,
                    ORDER_ID to incomeOrderId,
                    LEAVES_QTY to message.getField(ORDER_QTY)!!,
                    TEXT to "Simulated New Order Buy is placed",
                    EXEC_TYPE to "0",
                    ORD_STATUS to "0",
                    CUM_QTY to "0"
                ).build()

            context.send(
                fixNew.toBuilder()
                    .addField(EXEC_ID, execId.incrementAndGet())
                    .with(sessionAlias = aliases[KEY_ALIAS_1])
            )
            context.send(
                fixNew.toBuilder()
                    .addField(EXEC_ID, execId.incrementAndGet())
                    .with(sessionAlias = aliases[KEY_DC_ALIAS_1])
            )
        } else {
            book.addSell(incomeOrderId, handleTimestamp, message)
        }

        val sellOrder: BookRecord
        val firstBuyOrder: BookRecord
        val secondBuyOrder: BookRecord

        book.withLock {
            if (buySize < 2 || sellIsEmpty) {
                // we don't have enough orders for matching
                if (!sellIsEmpty) {
                    sellOrder = pullSell(handleTimestamp)
                    val expired = message("ExecutionReport")
                        .copyFields(
                            sellOrder.order,
                            ACCOUNT_TYPE, CL_ORD_ID, ORDER_CAPACITY, ORD_TYPE, PRICE, SECONDARY_CL_ORD_ID,
                            SECURITY_ID, SECURITY_ID_SOURCE, SIDE, TIME_IN_FORCE, TRADING_PARTY,
                        ).addFields(
                            ORDER_ID to sellOrder.orderId,
                            TRANSACT_TIME to handleTimestamp,
                            EXEC_TYPE to "C",
                            ORD_STATUS to "C",
                            CUM_QTY to calcQtyBuy(),
                            LEAVES_QTY to "0",
                            ORDER_QTY to sellOrder.order.getString(ORDER_QTY)!!,
                            EXEC_ID to execId.incrementAndGet(),
                            TEXT to "The remaining part of simulated order has been expired"
                        ).build()
                    context.send(expired.toBuilder().with(sessionAlias = aliases[KEY_ALIAS_2]))
                    //DropCopy
                    context.send(expired.toBuilder().with(sessionAlias = aliases[KEY_DC_ALIAS_2]))
                }
                return
            }
            // Useful variables for buy-side
            sellOrder = pullSell(handleTimestamp)
            firstBuyOrder = pullBuy(handleTimestamp)
            secondBuyOrder = pullBuy(handleTimestamp)
        }

        val cumQty1 = secondBuyOrder.order.getInt(ORDER_QTY)!!
        val cumQty2 = firstBuyOrder.order.getInt(ORDER_QTY)!!
        val leavesQty2 = sellOrder.order.getInt(ORDER_QTY)!! - (cumQty1 + cumQty2)
        val order1Price = firstBuyOrder.order.getString(PRICE)!!
        val order2Price = secondBuyOrder.order.getString(PRICE)!!

        val tradeMatchId1 = matchId.incrementAndGet()
        val tradeMatchId2 = matchId.incrementAndGet()

        // Generator ER
        // ER FF Order2 for Trader1
        val noPartyIdsTrader2Order3 = hashMapOf(
            NO_PARTY_IDS to createNoPartyIds("DEMO-CONN2", "DEMOFIRM1")
        )

        val trader1 = message("ExecutionReport")
            .copyFields(
                sellOrder.order,
                SECURITY_ID,
                SECURITY_ID_SOURCE,
                ORD_TYPE,
                ORDER_CAPACITY,
                ACCOUNT_TYPE
            ).addFields(
                TRADING_PARTY to hashMapOf(
                    NO_PARTY_IDS to createNoPartyIds("DEMO-CONN1", "DEMOFIRM2")
                ),
                SIDE to SIDE_BUY,
                TIME_IN_FORCE to "0",  // Get from message?
                EXEC_TYPE to "F",
                AGGRESSOR_INDICATOR to "N",
                ORD_STATUS to "2",
                LEAVES_QTY to 0,
                TEXT to "The simulated order has been fully traded"
            ).build()

        val trader1Order2 = trader1.toBuilder()
            .copyFields(
                secondBuyOrder.order,
                CL_ORD_ID,
                SECONDARY_CL_ORD_ID,
                ORDER_QTY
            ).addFields(
                TRANSACT_TIME to handleTimestamp,
                CUM_QTY to cumQty1,
                PRICE to order2Price,
                LAST_PX to order2Price,
                ORDER_ID to secondBuyOrder.orderId,
                EXEC_ID to execId.incrementAndGet(),
                TRD_MATCH_ID to tradeMatchId1
            ).build()

        context.send(trader1Order2.toBuilder().with(sessionAlias = aliases[KEY_ALIAS_1]))
        context.send(trader1Order2.toBuilder().with(sessionAlias = aliases[KEY_DC_ALIAS_1]))

        // ER FF Order1 for Trader1
        val trader1Order1 = trader1.toBuilder()
            .copyFields(
                firstBuyOrder.order,
                CL_ORD_ID, ORDER_QTY, PRICE, SECONDARY_CL_ORD_ID,
            ).addFields(
                TRANSACT_TIME to handleTimestamp,
                CUM_QTY to cumQty2,
                LAST_PX to firstBuyOrder.order.getField(PRICE),
                ORDER_ID to firstBuyOrder.orderId,
                EXEC_ID to execId.incrementAndGet(),
                TRD_MATCH_ID to tradeMatchId2,
            ).build()

        context.send(trader1Order1.toBuilder().with(sessionAlias = aliases[KEY_ALIAS_1]))
        context.send(trader1Order1.toBuilder().with(sessionAlias = aliases[KEY_DC_ALIAS_1]))

        val trader2 = message("ExecutionReport").copyFields(
            sellOrder.order,
            TIME_IN_FORCE,
            SIDE,
            PRICE,
            CL_ORD_ID,
            SECONDARY_CL_ORD_ID,
            SECURITY_ID,
            SECURITY_ID_SOURCE,
            ORD_TYPE,
            ORDER_CAPACITY,
            ACCOUNT_TYPE
        ).addFields(
            ORDER_ID to sellOrder.orderId
        ).build()

        val trader2Order3 = trader2.toBuilder()
            .addFields(
                TRADING_PARTY to noPartyIdsTrader2Order3,
                EXEC_TYPE to "F",
                AGGRESSOR_INDICATOR to "Y",
                ORD_STATUS to "1",
                ORDER_QTY to sellOrder.order.getString(ORDER_QTY)!!,
                TEXT to "The simulated order has been partially traded"
            ).build()

        val trader2Order3Er1 = trader2Order3.toBuilder()
            .addFields(
                TRANSACT_TIME to handleTimestamp,
                LAST_PX to order2Price,
                CUM_QTY to cumQty1,
                LEAVES_QTY to sellOrder.order.getInt(ORDER_QTY)!! - cumQty1,
                EXEC_ID to execId.incrementAndGet(),
                TRD_MATCH_ID to tradeMatchId1,
            ).build()

        // ER1 PF Order3 for Trader2
        context.send(trader2Order3Er1.toBuilder().with(sessionAlias = aliases[KEY_ALIAS_2]))
        //DropCopy
        context.send(trader2Order3Er1.toBuilder().with(sessionAlias = aliases[KEY_DC_ALIAS_2]))

        // ER2 PF Order3 for Trader2
        val trader2Order3Er2 = trader2Order3.toBuilder().addFields(
            TRANSACT_TIME to handleTimestamp,
            LAST_PX to order1Price,
            CUM_QTY to cumQty1 + cumQty2,
            LEAVES_QTY to leavesQty2,
            EXEC_ID to execId.incrementAndGet(),
            TRD_MATCH_ID to tradeMatchId2,
        ).build()

        context.send(trader2Order3Er2.toBuilder().apply {
            with(sessionAlias = aliases[KEY_ALIAS_2])
            if (instrument == "INSTR5") {
                addFields(
                    TEXT to "Execution Report with incorrect value in OrdStatus tag",
                    ORDER_CAPACITY to "P",  // Incorrect value as testcase
                    ACCOUNT_TYPE to "2"     // Incorrect value as testcase
                )
            }
        })

        //DropCopy
        context.send(trader2Order3Er2.toBuilder().with(sessionAlias = aliases[KEY_DC_ALIAS_2]))

        if (instrument == "INSTR4") {
            // Extra ER3 FF Order3 for Trader2 as testcase
            val trader2Order3fixX = trader2.toBuilder()
                .addFields(
                    TRANSACT_TIME to handleTimestamp,
                    TRADING_PARTY to noPartyIdsTrader2Order3,
                    EXEC_TYPE to "F",
                    AGGRESSOR_INDICATOR to "Y",
                    ORD_STATUS to "2",
                    LAST_PX to order1Price,
                    CUM_QTY to cumQty1 + cumQty2,
                    ORDER_QTY to sellOrder.order.getString(ORDER_QTY)!!,
                    LEAVES_QTY to leavesQty2,
                    EXEC_ID to execId.incrementAndGet(),
                    TRD_MATCH_ID to tradeMatchId2,
                    TEXT to "Extra Execution Report"
                )
            context.send(trader2Order3fixX.with(sessionAlias = aliases[KEY_ALIAS_2]))
        }

        // ER3 CC Order3 for Trader2
        val trader2Order3Er3CC = trader2.toBuilder()
            .copyFields(sellOrder.order, TRADING_PARTY)
            .addFields(
                TRANSACT_TIME to handleTimestamp,
                EXEC_TYPE to "C",
                ORD_STATUS to "C",
                CUM_QTY to cumQty1 + cumQty2,
                LEAVES_QTY to "0",
                ORDER_QTY to sellOrder.order.getString(ORDER_QTY)!!,
                EXEC_ID to execId.incrementAndGet(),
                TEXT to "The remaining part of simulated order has been expired"
            ).build()
        context.send(trader2Order3Er3CC.toBuilder().with(sessionAlias = aliases[KEY_ALIAS_2]))
        //DropCopy
        context.send(trader2Order3Er3CC.toBuilder().with(sessionAlias = aliases[KEY_DC_ALIAS_2]))
    }

    private fun sendReject(
        context: IRuleContext,
        message: ParsedMessage,
        refTagId: String,
    ) {
        context.send(
            message("Reject")
                .addFields(
                    REF_TAG_ID to refTagId,
                    REF_MSG_TYPE to message.getFieldSoft(HEADER, MSG_TYPE),
                    REF_SEQ_NUM to message.getFieldSoft(HEADER, MSG_SEQ_NUM),
                    TEXT to "Simulating reject message",
                    SESSION_REJECT_REASON to SESSION_REJECT_REASON_REQUIRED_TAG_MISSING
                ).with(sessionAlias = message.id.sessionAlias)
        )
    }

    @Suppress("SameParameterValue")
    private fun sendBusinessMessageReject(
        context: IRuleContext,
        message: ParsedMessage,
        refTagId: String,
    ) {
        context.send(
            message("BusinessMessageReject")
                .addFields(
                    REF_TAG_ID to refTagId,
                    REF_MSG_TYPE to message.getFieldSoft(HEADER, MSG_TYPE),
                    REF_SEQ_NUM to message.getFieldSoft(HEADER, MSG_SEQ_NUM),
                    TEXT to "Unknown SecurityID",
                    BUSINESS_REJECT_REASON to BUSINESS_REJECT_REASON_UNKNOWN_SECURITY,
                    BUSINESS_REJECT_REF_ID to message.getField(CL_ORD_ID)
                ).with(sessionAlias = message.id.sessionAlias)
        )
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
            PARTY_ROLE to partyRole,
            PARTY_ID to partyID,
            PARTY_ID_SOURCE to partyIDSource,
        )

    private fun ParsedMessage.FromMapBuilder.with(sessionAlias: String? = null): ParsedMessage.FromMapBuilder = apply {
        with(idBuilder()) {
            sessionAlias?.let(this::setSessionAlias)
        }
    }
}

private data class BookRecord(val orderId: Int, val timestamp: Instant, val order: ParsedMessage)

@Suppress("unused")
private class Book(
    private val log: BookLog?
) {
    private val lock = ReentrantLock()
    private val buy: Queue<BookRecord> = LinkedList()
    private val sell: Queue<BookRecord> = LinkedList()

    val buyIsEmpty: Boolean
        get() = buy.isEmpty()

    val buySize: Int
        get() = buy.size

    val sellSize: Int
        get() = sell.size

    val sellIsEmpty: Boolean
        get() = sell.isEmpty()

    fun addBuy(orderId: Int, timestamp: Instant, order: ParsedMessage) =
        lock.withLock { buy.add(BookRecord(orderId, timestamp, order).log(ADD, timestamp, "BUY")) }
    fun addSell(orderId: Int, timestamp: Instant, order: ParsedMessage) =
        lock.withLock { sell.add(BookRecord(orderId, timestamp, order).log(ADD, timestamp, "SELL")) }

    fun pullBuy(timestamp: Instant): BookRecord = lock.withLock { buy.remove().log(DELETE, timestamp, "BUY") }
    fun pullSell(timestamp: Instant): BookRecord = lock.withLock { sell.remove().log(DELETE, timestamp, "SELL") }

    fun calcQtyBuy(): Int = lock.withLock { calcQty(buy) }

    fun calcQtySell(): Int = lock.withLock { calcQty(sell) }

    @OptIn(ExperimentalContracts::class)
    inline fun withLock(func: Book.() -> Unit) {
        contract { callsInPlace(func, InvocationKind.EXACTLY_ONCE) }
        lock.withLock { func() }
    }

    private fun BookRecord.log(action: Action, timestamp: Instant, side: String): BookRecord = this.also {
        log?.log(
            action, timestamp, order.getString(CL_ORD_ID), orderId.toString(), order.getString(SECURITY_ID),
            side, order.getString(PRICE), order.getString(ORDER_QTY)
        )
    }

    companion object {
        private fun calcQty(queue: Queue<BookRecord>): Int = queue.sumOf { it.order.body.getInt(ORDER_QTY)!! }
    }
}

private enum class Action {
    ADD,
    DELETE,
}

private interface BookLog {
    fun log(
        action: Action,
        transactTime: Instant,
        clOrdID: String?,
        ordID: String?,
        instrument: String?,
        side: String?,
        price: String?,
        qty: String?,
    )
}

private class CsvBookLog(path: Path) : BookLog {
    private val writer = CSVWriter(FileWriter(path.toFile()))

    init {
        writer.writeNext(arrayOf("Action", "TransactTime", "ClOrdID", "OrdID", "Instrument", "Side", "Price", "Qty"))
        writer.flush()
    }

    override fun log(
        action: Action,
        transactTime: Instant,
        clOrdID: String?,
        ordID: String?,
        instrument: String?,
        side: String?,
        price: String?,
        qty: String?,
    ) {
        writer.writeNext(arrayOf(action.name, transactTime.toString(), clOrdID, ordID, instrument, side, price, qty))
        writer.flush()
    }

    companion object {
        private const val CSV_EXTENSION = "csv"

        fun createCsvBookLog(dirPath: Path?, pattern: String?): BookLog? {
            if (dirPath == null || pattern == null) return null
            if (dirPath.notExists()) dirPath.createDirectories()
            dirPath.listDirectoryEntries()
                .filter { it.name.startsWith(pattern) && it.extension == CSV_EXTENSION }
                .sortedByDescending { Files.getLastModifiedTime(it).toInstant() }
                .drop(1)
                .forEach {
                    try {
                        Files.delete(it)
                        LOGGER.info { "removed file: $it" }
                    } catch (e: Exception) {
                        LOGGER.error(e) { "removing file: $it failure" }
                    }
                }

            val bookLogFile = dirPath.resolve("$pattern-${System.currentTimeMillis()}.$CSV_EXTENSION")
            return CsvBookLog(bookLogFile).also {
                LOGGER.info { "created book log for file $bookLogFile" }
            }
        }
    }
}