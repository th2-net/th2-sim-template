/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.builders.MapBuilder
import com.exactpro.th2.common.utils.message.transport.addFields
import com.exactpro.th2.common.utils.message.transport.message
import com.exactpro.th2.sim.template.FixFields.Companion.ACCOUNT_TYPE
import com.exactpro.th2.sim.template.FixFields.Companion.AGGRESSOR_INDICATOR
import com.exactpro.th2.sim.template.FixFields.Companion.BEGIN_STRING
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
import com.exactpro.th2.sim.template.FixFields.Companion.PARTY_ID
import com.exactpro.th2.sim.template.FixFields.Companion.PARTY_ID_SOURCE
import com.exactpro.th2.sim.template.FixFields.Companion.PARTY_ROLE
import com.exactpro.th2.sim.template.FixFields.Companion.PRICE
import com.exactpro.th2.sim.template.FixFields.Companion.REF_MSG_TYPE
import com.exactpro.th2.sim.template.FixFields.Companion.REF_SEQ_NUM
import com.exactpro.th2.sim.template.FixFields.Companion.REF_TAG_ID
import com.exactpro.th2.sim.template.FixFields.Companion.SECURITY_ID
import com.exactpro.th2.sim.template.FixFields.Companion.SESSION_REJECT_REASON
import com.exactpro.th2.sim.template.FixFields.Companion.SIDE
import com.exactpro.th2.sim.template.FixFields.Companion.TEXT
import com.exactpro.th2.sim.template.FixFields.Companion.TIME_IN_FORCE
import com.exactpro.th2.sim.template.FixFields.Companion.TRADING_PARTY
import com.exactpro.th2.sim.template.FixFields.Companion.TRANSACT_TIME
import com.exactpro.th2.sim.template.FixFields.Companion.TRD_MATCH_ID
import com.exactpro.th2.sim.template.FixValues.Companion.ACCOUNT_TYPE_NON_CUSTOMER
import com.exactpro.th2.sim.template.FixValues.Companion.BUSINESS_REJECT_REASON_UNKNOWN_SECURITY
import com.exactpro.th2.sim.template.FixValues.Companion.EXEC_TYPE_EXPIRED
import com.exactpro.th2.sim.template.FixValues.Companion.EXEC_TYPE_NEW
import com.exactpro.th2.sim.template.FixValues.Companion.EXEC_TYPE_TRADE
import com.exactpro.th2.sim.template.FixValues.Companion.ORDER_CAPACITY_PRINCIPAL
import com.exactpro.th2.sim.template.FixValues.Companion.ORD_STATUS_EXPIRED
import com.exactpro.th2.sim.template.FixValues.Companion.ORD_STATUS_FILLED
import com.exactpro.th2.sim.template.FixValues.Companion.ORD_STATUS_NEW
import com.exactpro.th2.sim.template.FixValues.Companion.ORD_STATUS_PARTIALLY_FILLED
import com.exactpro.th2.sim.template.FixValues.Companion.PARTY_ID_SOURCE_PROPRIETARY_CUSTOM_CODE
import com.exactpro.th2.sim.template.FixValues.Companion.PARTY_ID_SOURCE_SHORT_CODE_IDENTIFIER
import com.exactpro.th2.sim.template.FixValues.Companion.PARTY_ROLE_CLIENT_ID
import com.exactpro.th2.sim.template.FixValues.Companion.PARTY_ROLE_CONTRA_FIRM
import com.exactpro.th2.sim.template.FixValues.Companion.PARTY_ROLE_DECK_ID
import com.exactpro.th2.sim.template.FixValues.Companion.PARTY_ROLE_EXECUTING_TRADER
import com.exactpro.th2.sim.template.FixValues.Companion.PARTY_ROLE_INVESTMENT_DIVISION_MARKER
import com.exactpro.th2.sim.template.FixValues.Companion.SESSION_REJECT_REASON_REQUIRED_TAG_MISSING
import com.exactpro.th2.sim.template.FixValues.Companion.SIDE_BUY
import com.exactpro.th2.sim.template.FixValues.Companion.SIDE_SELL
import com.exactpro.th2.sim.template.FixValues.Companion.TIME_IN_FORCE_DAY
import com.exactpro.th2.sim.template.rule.test.api.TestRuleContext.Companion.testRule
import com.opencsv.CSVReader
import org.apache.commons.lang3.RandomStringUtils.insecure
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import strikt.api.Assertion
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isBlank
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThan
import strikt.assertions.isLessThan
import strikt.assertions.isNotNull
import java.io.FileReader
import java.nio.file.Path
import java.time.Instant
import kotlin.io.path.absolutePathString
import kotlin.io.path.exists
import kotlin.io.path.extension
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.random.Random


class KotlinFIXRuleTest {
    private val rule = createRule()

    @BeforeEach
    fun beforeEach() {
        testRule { rule.touch(this, mapOf("action" to "rEsEt")) }
    }

    @Test // FIXME: several rules can affect each other
    fun `csv book writer remove old files after create new rule test`(@TempDir tempDir: Path) {
        val pattern = "test-file"
        System.setProperty("th2.sim.kotlin-fix-rule.book-log.dir", tempDir.toString())
        System.setProperty("th2.sim.kotlin-fix-rule.book-log.pattern", pattern)

        val files = (0..3).associate {
            tempDir.resolve("$pattern-${insecure().nextAlphanumeric(10)}.csv") to insecure().nextAlphanumeric(10)
        }

        files.forEach { path, content ->
            path.writeText(content)
            Thread.sleep(1) // Files.getLastModifiedTime returns time with microseconds precession
        }

        assertAll(
            files.keys.map { path ->
                { assertTrue(path.exists(), path.absolutePathString()) }
            }
        )

        val checkpoint = Instant.now()
        createRule()
        expectCsvFiles(tempDir, files.toList(), pattern, checkpoint)
    }

    @Test
    fun `csv book writer remove old files after reset rule test`(@TempDir tempDir: Path) {
        val pattern = "test-file"
        System.setProperty("th2.sim.kotlin-fix-rule.book-log.dir", tempDir.toString())
        System.setProperty("th2.sim.kotlin-fix-rule.book-log.pattern", pattern)

        val files = (0..3).associate {
            tempDir.resolve("$pattern-${insecure().nextAlphanumeric(10)}.csv") to insecure().nextAlphanumeric(10)
        }

        files.forEach { path, content ->
            path.writeText(content)
            Thread.sleep(1) // Files.getLastModifiedTime returns time with microseconds precession
        }

        assertAll(
            files.keys.map { path ->
                { assertTrue(path.exists(), path.absolutePathString()) }
            }
        )

        val checkpoint = Instant.now()
        testRule { rule.touch(this, mapOf("action" to "reset")) }
        expectCsvFiles(tempDir, files.toList(), pattern, checkpoint)
    }

    @Test
    fun `csv book writer remove old files after nos with reset text test`(@TempDir tempDir: Path) {
        val pattern = "test-file"
        System.setProperty("th2.sim.kotlin-fix-rule.book-log.dir", tempDir.toString())
        System.setProperty("th2.sim.kotlin-fix-rule.book-log.pattern", pattern)

        val files = (0..3).associate {
            tempDir.resolve("$pattern-${insecure().nextAlphanumeric(10)}.csv") to insecure().nextAlphanumeric(10)
        }

        files.forEach { path, content ->
            path.writeText(content)
            Thread.sleep(1) // Files.getLastModifiedTime returns time with microseconds precession
        }

        assertAll(
            files.keys.map { path ->
                { assertTrue(path.exists(), path.absolutePathString()) }
            }
        )

        val checkpoint = Instant.now()
        testRule {
            val nos = buildMessage {
                addField(TEXT, "ReSeT")
            }
            rule.assertHandle(nos)
            assertNothingSent()
        }
        expectCsvFiles(tempDir, files.toList(), pattern, checkpoint)
    }

    @Test
    fun `touch reset internal state test`() {
        testRule {
            val buy1 = buildMessage(SIDE_BUY, "test-security")
            val buy2 = buildMessage(SIDE_BUY, "test-security")
            val sell1 = buildMessage(SIDE_SELL, "test-security")

            rule.assertHandle(buy1)
            rule.assertHandle(buy2)
            rule.touch(this, mapOf("action" to "reset"))
            rule.assertHandle(sell1)

            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy1, msg, ALIAS_1, orderId = 1, execId = 1) }
            assertSent(BUILDER_CLASS) { msg ->
                expectNewBuyIsPlaced(buy1, msg, DC_ALIAS_1, orderId = 1, execId = 2) // execId looks as bag
            }
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy2, msg, ALIAS_1, orderId = 2, execId = 3) }

            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy2, msg, DC_ALIAS_1, orderId = 2, execId = 4) }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell1, null, null, msg, ALIAS_2, orderId = 1, execId = 1)
            }

            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell1, null, null, msg, DC_ALIAS_2, orderId = 1, execId = 1)
            }
            assertNothingSent()
        }
    }

    @Test
    fun `two buy one sell for test-security with book log test`(@TempDir tempDir: Path) {
        val pattern = "test-file"
        System.setProperty("th2.sim.kotlin-fix-rule.book-log.dir", tempDir.toString())
        System.setProperty("th2.sim.kotlin-fix-rule.book-log.pattern", pattern)

        testRule {
            rule.touch(this, mapOf("action" to "reset"))
            val buy1 = buildMessage(SIDE_BUY, "test-security")
            val buy2 = buildMessage(SIDE_BUY, "test-security")
            val sell1 = buildMessage(SIDE_SELL, "test-security")

            val timestamp1 = Instant.now()
            rule.assertHandle(buy1)

            Thread.sleep(1) // Files.getLastModifiedTime returns time with microseconds precession
            val timestamp2 = Instant.now()
            rule.assertHandle(buy2)

            Thread.sleep(1) // Files.getLastModifiedTime returns time with microseconds precession
            val timestamp3 = Instant.now()
            rule.assertHandle(sell1)

            var buyEr1: ParsedMessage? = null
            var buyTrd1: ParsedMessage? = null
            var buyEr2: ParsedMessage? = null
            var buyTrd2: ParsedMessage? = null
            var sellEr1: ParsedMessage? = null
            var sellTrd1: ParsedMessage? = null

            assertSent(BUILDER_CLASS) { msg ->
                buyEr1 = msg.build()
                expectNewBuyIsPlaced(buy1, msg, ALIAS_1, orderId = 1, execId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectNewBuyIsPlaced(buy1, msg, DC_ALIAS_1, orderId = 1, execId = 2) // execId looks as bag
            }
            assertSent(BUILDER_CLASS) { msg ->
                buyEr2 = msg.build()
                expectNewBuyIsPlaced(buy2, msg, ALIAS_1, orderId = 2, execId = 3)
            }
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy2, msg, DC_ALIAS_1, orderId = 2, execId = 4) }
            assertSent(BUILDER_CLASS) { msg ->
                buyTrd2 = msg.build()
                expectOrderFullFilled(buy2, msg, ALIAS_1, orderId = 2, execId = 5, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy2, msg, DC_ALIAS_1, orderId = 2, execId = 5, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                buyTrd1 = msg.build()
                expectOrderFullFilled(buy1, msg, ALIAS_1, orderId = 1, execId = 6, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy1, msg, DC_ALIAS_1, orderId = 1, execId = 6, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                sellTrd1 = msg.build()
                expectOrderPartiallyTraded(sell1, buy2, null, msg, ALIAS_2, orderId = 3, execId = 7, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy2, null, msg, DC_ALIAS_2, orderId = 3, execId = 7, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy1, buy2, msg, ALIAS_2, orderId = 3, execId = 8, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy1, buy2, msg, DC_ALIAS_2, orderId = 3, execId = 8, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                sellEr1 = msg.build()
                expectOrderExpired(sell1, buy1, buy2, msg, ALIAS_2, orderId = 3, execId = 9)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell1, buy1, buy2, msg, DC_ALIAS_2, orderId = 3, execId = 9)
            }
            assertNothingSent()

            println(buy1.body[TRANSACT_TIME])
            println(buy2.body[TRANSACT_TIME])
            println(sell1.body[TRANSACT_TIME])
            println(buyEr1?.body[TRANSACT_TIME])
            println(buyEr2?.body[TRANSACT_TIME])
            println(sellEr1?.body[TRANSACT_TIME])

            val files = tempDir.listDirectoryEntries().toList()
            assertEquals(1, files.size, "check size")
            val lines = CSVReader(FileReader(files.single().toFile())).use(CSVReader::readAll)
            expectThat(lines) {
                get { size } isEqualTo 8
                get { get(0) } and {
                    get { size } isEqualTo 8
                    get { get(0) } isEqualTo "Action"
                    get { get(1) } isEqualTo "TransactTime"
                    get { get(2) } isEqualTo "ClOrdID"
                    get { get(3) } isEqualTo "OrdID"
                    get { get(4) } isEqualTo "Instrument"
                    get { get(5) } isEqualTo "Side"
                    get { get(6) } isEqualTo "Price"
                    get { get(7) } isEqualTo "Qty"
                }
                get { get(1) } and {
                    get { size } isEqualTo 8
                    get { get(0) } isEqualTo "FULL_RESET"
                    get { get(1) }.isNotNull() and {
                        get { Instant.parse(this) }.isLessThan(timestamp1)
                    }
                    get { get(2) }.isBlank()
                    get { get(3) }.isBlank()
                    get { get(4) }.isBlank()
                    get { get(5) }.isBlank()
                    get { get(6) }.isBlank()
                    get { get(7) }.isBlank()
                }
                get { get(2) } and {
                    get { size } isEqualTo 8
                    get { get(0) } isEqualTo "ADD"
                    get { get(1) } isEqualTo buyEr1?.body[TRANSACT_TIME]?.toString() and {
                        get { Instant.parse(this) }.isGreaterThan(timestamp1)
                    }
                    get { get(2) } isEqualTo buy1.body[CL_ORD_ID]?.toString()
                    get { get(3) } isEqualTo buyEr1?.body[ORDER_ID]?.toString()
                    get { get(4) } isEqualTo buy1.body[SECURITY_ID]?.toString()
                    get { get(5) } isEqualTo "BUY"
                    get { get(6) } isEqualTo buy1.body[PRICE]?.toString()
                    get { get(7) } isEqualTo buy1.body[ORDER_QTY]?.toString()
                }
                get { get(3) } and {
                    get { size } isEqualTo 8
                    get { get(0) } isEqualTo "ADD"
                    get { get(1) } isEqualTo buyEr2?.body[TRANSACT_TIME]?.toString() and {
                        get { Instant.parse(this) }.isGreaterThan(timestamp2)
                    }
                    get { get(2) } isEqualTo buy2.body[CL_ORD_ID]?.toString()
                    get { get(3) } isEqualTo buyEr2?.body[ORDER_ID]?.toString()
                    get { get(4) } isEqualTo buy2.body[SECURITY_ID]?.toString()
                    get { get(5) } isEqualTo "BUY"
                    get { get(6) } isEqualTo buy2.body[PRICE]?.toString()
                    get { get(7) } isEqualTo buy2.body[ORDER_QTY]?.toString()
                }
                get { get(4) } and {
                    get { size } isEqualTo 8
                    get { get(0) } isEqualTo "ADD"
                    get { get(1) } isEqualTo sellEr1?.body[TRANSACT_TIME]?.toString() and {
                        get { Instant.parse(this) }.isGreaterThan(timestamp3)
                    }
                    get { get(2) } isEqualTo sell1.body[CL_ORD_ID]?.toString()
                    get { get(3) } isEqualTo sellEr1?.body[ORDER_ID]?.toString()
                    get { get(4) } isEqualTo sell1.body[SECURITY_ID]?.toString()
                    get { get(5) } isEqualTo "SELL"
                    get { get(6) } isEqualTo sell1.body[PRICE]?.toString()
                    get { get(7) } isEqualTo sell1.body[ORDER_QTY]?.toString()
                }
                get { get(5) } and {
                    get { size } isEqualTo 8
                    get { get(0) } isEqualTo "DELETE"
                    get { get(1) } isEqualTo sellTrd1?.body[TRANSACT_TIME]?.toString() and {
                        get { Instant.parse(this) }.isGreaterThan(timestamp3)
                    }
                    get { get(2) } isEqualTo sell1.body[CL_ORD_ID]?.toString()
                    get { get(3) } isEqualTo sellEr1?.body[ORDER_ID]?.toString()
                    get { get(4) } isEqualTo sell1.body[SECURITY_ID]?.toString()
                    get { get(5) } isEqualTo "SELL"
                    get { get(6) } isEqualTo sell1.body[PRICE]?.toString()
                    get { get(7) } isEqualTo sell1.body[ORDER_QTY]?.toString()
                }
                get { get(6) } and {
                    get { size } isEqualTo 8
                    get { get(0) } isEqualTo "DELETE"
                    get { get(1) } isEqualTo buyTrd1?.body[TRANSACT_TIME]?.toString() and {
                        get { Instant.parse(this) }.isGreaterThan(timestamp3)
                    }
                    get { get(2) } isEqualTo buy1.body[CL_ORD_ID]?.toString()
                    get { get(3) } isEqualTo buyEr1?.body[ORDER_ID]?.toString()
                    get { get(4) } isEqualTo buy1.body[SECURITY_ID]?.toString()
                    get { get(5) } isEqualTo "BUY"
                    get { get(6) } isEqualTo buy1.body[PRICE]?.toString()
                    get { get(7) } isEqualTo buy1.body[ORDER_QTY]?.toString()
                }
                get { get(7) } and {
                    get { size } isEqualTo 8
                    get { get(0) } isEqualTo "DELETE"
                    get { get(1) } isEqualTo buyTrd2?.body[TRANSACT_TIME]?.toString() and {
                        get { Instant.parse(this) }.isGreaterThan(timestamp3)
                    }
                    get { get(2) } isEqualTo buy2.body[CL_ORD_ID]?.toString()
                    get { get(3) } isEqualTo buyEr2?.body[ORDER_ID]?.toString()
                    get { get(4) } isEqualTo buy2.body[SECURITY_ID]?.toString()
                    get { get(5) } isEqualTo "BUY"
                    get { get(6) } isEqualTo buy2.body[PRICE]?.toString()
                    get { get(7) } isEqualTo buy2.body[ORDER_QTY]?.toString()
                }
            }
        }
    }

    @Test
    fun `two buy one sell for test-security test`() {
        testRule {
            val buy1 = buildMessage(SIDE_BUY, "test-security")
            val buy2 = buildMessage(SIDE_BUY, "test-security")
            val sell1 = buildMessage(SIDE_SELL, "test-security")
            rule.assertHandle(buy1)
            rule.assertHandle(buy2)
            rule.assertHandle(sell1)

            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy1, msg, ALIAS_1, orderId = 1, execId = 1) }
            assertSent(BUILDER_CLASS) { msg ->
                expectNewBuyIsPlaced(buy1, msg, DC_ALIAS_1, orderId = 1, execId = 2) // execId looks as bag
            }
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy2, msg, ALIAS_1, orderId = 2, execId = 3) }
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy2, msg, DC_ALIAS_1, orderId = 2, execId = 4) }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy2, msg, ALIAS_1, orderId = 2, execId = 5, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy2, msg, DC_ALIAS_1, orderId = 2, execId = 5, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy1, msg, ALIAS_1, orderId = 1, execId = 6, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy1, msg, DC_ALIAS_1, orderId = 1, execId = 6, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy2, null, msg, ALIAS_2, orderId = 3, execId = 7, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy2, null, msg, DC_ALIAS_2, orderId = 3, execId = 7, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy1, buy2, msg, ALIAS_2, orderId = 3, execId = 8, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy1, buy2, msg, DC_ALIAS_2, orderId = 3, execId = 8, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell1, buy1, buy2, msg, ALIAS_2, orderId = 3, execId = 9)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell1, buy1, buy2, msg, DC_ALIAS_2, orderId = 3, execId = 9)
            }
            assertNothingSent()
        }
    }

    @Test
    fun `one buy two sell for test-security test`() {
        testRule {
            val buy1 = buildMessage(SIDE_BUY, "test-security")
            val sell1 = buildMessage(SIDE_SELL, "test-security")
            val sell2 = buildMessage(SIDE_SELL, "test-security")
            rule.assertHandle(sell1)
            rule.assertHandle(buy1)
            rule.assertHandle(sell2)

            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell1, null, null, msg, ALIAS_2, orderId = 1, execId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell1, null, null, msg, DC_ALIAS_2, orderId = 1, execId = 1)
            }
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy1, msg, ALIAS_1, orderId = 2, execId = 2) }
            assertSent(BUILDER_CLASS) { msg ->
                expectNewBuyIsPlaced(buy1, msg, DC_ALIAS_1, orderId = 2, execId = 3) // execId looks as bag
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell2, buy1, null, msg, ALIAS_2, orderId = 3, execId = 4)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell2, buy1, null, msg, DC_ALIAS_2, orderId = 3, execId = 4)
            }
            assertNothingSent()
        }
    }

    @Test
    fun `two buy one sell for two security ids test`() {
        testRule {
            val buyA1 = buildMessage(SIDE_BUY, "test-security-a")
            val buyA2 = buildMessage(SIDE_BUY, "test-security-a")
            val sellA1 = buildMessage(SIDE_SELL, "test-security-a")
            val buyB1 = buildMessage(SIDE_BUY, "test-security-b")
            val buyB2 = buildMessage(SIDE_BUY, "test-security-b")
            val sellB1 = buildMessage(SIDE_SELL, "test-security-b")

            rule.assertHandle(buyA1)
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buyA1, msg, ALIAS_1, orderId = 1, execId = 1) }
            assertSent(BUILDER_CLASS) { msg ->
                expectNewBuyIsPlaced(buyA1, msg, DC_ALIAS_1, orderId = 1, execId = 2) // execId looks as bag
            }
            assertNothingSent()

            rule.assertHandle(buyB1)
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buyB1, msg, ALIAS_1, orderId = 2, execId = 3) }
            assertSent(BUILDER_CLASS) { msg ->
                expectNewBuyIsPlaced(buyB1, msg, DC_ALIAS_1, orderId = 2, execId = 4) // execId looks as bag
            }
            assertNothingSent()

            rule.assertHandle(buyB2)
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buyB2, msg, ALIAS_1, orderId = 3, execId = 5) }
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buyB2, msg, DC_ALIAS_1, orderId = 3, execId = 6) }
            assertNothingSent()

            rule.assertHandle(buyA2)
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buyA2, msg, ALIAS_1, orderId = 4, execId = 7) }
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buyA2, msg, DC_ALIAS_1, orderId = 4, execId = 8) }
            assertNothingSent()

            rule.assertHandle(sellA1)
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buyA2, msg, ALIAS_1, orderId = 4, execId = 9, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buyA2, msg, DC_ALIAS_1, orderId = 4, execId = 9, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buyA1, msg, ALIAS_1, orderId = 1, execId = 10, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buyA1, msg, DC_ALIAS_1, orderId = 1, execId = 10, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sellA1, buyA2, null, msg, ALIAS_2, orderId = 5, execId = 11, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sellA1, buyA2, null, msg, DC_ALIAS_2, orderId = 5, execId = 11, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sellA1, buyA1, buyA2, msg, ALIAS_2, orderId = 5, execId = 12, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sellA1, buyA1, buyA2, msg, DC_ALIAS_2, orderId = 5, execId = 12, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sellA1, buyA1, buyA2, msg, ALIAS_2, orderId = 5, execId = 13)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sellA1, buyA1, buyA2, msg, DC_ALIAS_2, orderId = 5, execId = 13)
            }
            assertNothingSent()

            rule.assertHandle(sellB1)
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buyB2, msg, ALIAS_1, orderId = 3, execId = 14, matchId = 3)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buyB2, msg, DC_ALIAS_1, orderId = 3, execId = 14, matchId = 3)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buyB1, msg, ALIAS_1, orderId = 2, execId = 15, matchId = 4)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buyB1, msg, DC_ALIAS_1, orderId = 2, execId = 15, matchId = 4)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sellB1, buyB2, null, msg, ALIAS_2, orderId = 6, execId = 16, matchId = 3)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sellB1, buyB2, null, msg, DC_ALIAS_2, orderId = 6, execId = 16, matchId = 3)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sellB1, buyB1, buyB2, msg, ALIAS_2, orderId = 6, execId = 17, matchId = 4)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sellB1, buyB1, buyB2, msg, DC_ALIAS_2, orderId = 6, execId = 17, matchId = 4)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sellB1, buyB1, buyB2, msg, ALIAS_2, orderId = 6, execId = 18)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sellB1, buyB1, buyB2, msg, DC_ALIAS_2, orderId = 6, execId = 18)
            }
            assertNothingSent()
        }
    }

    @Test
    fun `nos without side`() {
        testRule {
            val nos = buildMessage()
            rule.assertHandle(nos)
            assertSent(BUILDER_CLASS) { msg -> expectRejected(nos, msg, 453) }
            assertNothingSent()
        }
    }

    @ParameterizedTest
    @ValueSource(strings = [SIDE_SELL, SIDE_BUY])
    fun `nos without security id`(side: String) {
        testRule {
            val nos = buildMessage {
                addField(SIDE, side)
            }
            rule.assertHandle(nos)
            assertSent(BUILDER_CLASS) { msg -> expectRejected(nos, msg, 48) }
            assertNothingSent()
        }
    }

    @Test
    @DisplayName("test to check response of message with field SecurityID = INSTR4 and side = 1/2")
    fun `two buy one sell for INSTR4 test`() {
        testRule {
            val buy1 = buildMessage(SIDE_BUY, INSTR4)
            val buy2 = buildMessage(SIDE_BUY, INSTR4)
            val sell1 = buildMessage(SIDE_SELL, INSTR4)
            rule.assertHandle(buy1)
            rule.assertHandle(buy2)
            rule.assertHandle(sell1)

            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy1, msg, ALIAS_1, orderId = 1, execId = 1) }
            assertSent(BUILDER_CLASS) { msg ->
                expectNewBuyIsPlaced(buy1, msg, DC_ALIAS_1, orderId = 1, execId = 2) // execId looks as bag
            }
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy2, msg, ALIAS_1, orderId = 2, execId = 3) }
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy2, msg, DC_ALIAS_1, orderId = 2, execId = 4) }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy2, msg, ALIAS_1, orderId = 2, execId = 5, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy2, msg, DC_ALIAS_1, orderId = 2, execId = 5, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy1, msg, ALIAS_1, orderId = 1, execId = 6, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy1, msg, DC_ALIAS_1, orderId = 1, execId = 6, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy2, null, msg, ALIAS_2, orderId = 3, execId = 7, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy2, null, msg, DC_ALIAS_2, orderId = 3, execId = 7, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy1, buy2, msg, ALIAS_2, orderId = 3, execId = 8, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy1, buy2, msg, DC_ALIAS_2, orderId = 3, execId = 8, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectExtraER(sell1, buy1, buy2, msg, ALIAS_2, orderId = 3, execId = 9, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell1, buy1, buy2, msg, ALIAS_2, orderId = 3, execId = 10)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell1, buy1, buy2, msg, DC_ALIAS_2, orderId = 3, execId = 10)
            }
            assertNothingSent()
        }
    }

    @Test
    @DisplayName("test to check response of message with field SecurityID = INSTR5 and side = 1/2")
    fun `two buy one sell for INSTR5 test`() {
        testRule {
            val buy1 = buildMessage(SIDE_BUY, INSTR5)
            val buy2 = buildMessage(SIDE_BUY, INSTR5)
            val sell1 = buildMessage(SIDE_SELL, INSTR5)
            rule.assertHandle(buy1)
            rule.assertHandle(buy2)
            rule.assertHandle(sell1)

            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy1, msg, ALIAS_1, orderId = 1, execId = 1) }
            assertSent(BUILDER_CLASS) { msg ->
                expectNewBuyIsPlaced(buy1, msg, DC_ALIAS_1, orderId = 1, execId = 2) // execId looks as bag
            }
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy2, msg, ALIAS_1, orderId = 2, execId = 3) }
            assertSent(BUILDER_CLASS) { msg -> expectNewBuyIsPlaced(buy2, msg, DC_ALIAS_1, orderId = 2, execId = 4) }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy2, msg, ALIAS_1, orderId = 2, execId = 5, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy2, msg, DC_ALIAS_1, orderId = 2, execId = 5, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy1, msg, ALIAS_1, orderId = 1, execId = 6, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderFullFilled(buy1, msg, DC_ALIAS_1, orderId = 1, execId = 6, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy2, null, msg, ALIAS_2, orderId = 3, execId = 7, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy2, null, msg, DC_ALIAS_2, orderId = 3, execId = 7, matchId = 1)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectIncorrectValueInOrdStsTag(sell1, buy1, buy2, msg, ALIAS_2, orderId = 3, execId = 8, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderPartiallyTraded(sell1, buy1, buy2, msg, DC_ALIAS_2, orderId = 3, execId = 8, matchId = 2)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell1, buy1, buy2, msg, ALIAS_2, orderId = 3, execId = 9)
            }
            assertSent(BUILDER_CLASS) { msg ->
                expectOrderExpired(sell1, buy1, buy2, msg, DC_ALIAS_2, orderId = 3, execId = 9)
            }
            assertNothingSent()
        }
    }

    @ParameterizedTest
    @ValueSource(strings = [SIDE_SELL, SIDE_BUY])
    @DisplayName("test to check response of message with field SecurityID = INSTR6")
    fun `nos for INSTR6 test`(side: String) {
        testRule {
            val nos = buildMessage(side, INSTR6)
            rule.assertHandle(nos)
            assertSent(BUILDER_CLASS) { msg -> expectBMRejected(nos, msg) }
            assertNothingSent()
        }
    }

    companion object {
        private const val INSTR4 = "INSTR4"
        private const val INSTR5 = "INSTR5"
        private const val INSTR6 = "INSTR6"

        private const val ALIAS_1 = "ALIAS_1"
        private const val ALIAS_2 = "ALIAS_2"
        private const val DC_ALIAS_1 = "DC_ALIAS_1"
        private const val DC_ALIAS_2 = "DC_ALIAS_2"

        private const val EXECUTION_REPORT = "ExecutionReport"
        private const val BUSINESS_MESSAGE_REJECT = "BusinessMessageReject"
        private const val REJECT = "Reject"

        private const val TRIGGER_FIELD = "check"
        private val NOT_COPIED_FIELD = setOf(HEADER, TRANSACT_TIME, TRIGGER_FIELD)

        private val BUILDER_CLASS = ParsedMessage.FromMapBuilder::class.java

        private fun createRule(): KotlinFIXRule = KotlinFIXRule(
            mapOf(TRIGGER_FIELD to "true"),
            mapOf(
                "ALIAS_1" to ALIAS_1,
                "ALIAS_2" to ALIAS_2,
                "DC_ALIAS_1" to DC_ALIAS_1,
                "DC_ALIAS_2" to DC_ALIAS_2,
            )
        )

        private inline fun buildMessage(
            type: String = "NewOrderSingle",
            func: ParsedMessage.FromMapBuilder.() -> Unit = {}
        ): ParsedMessage =
            message(type) {
                idBuilder()
                    .setSessionAlias("test-alias")
                    .setDirection(Direction.OUTGOING)
                    .setSequence(System.currentTimeMillis())
                    .setTimestamp(Instant.now())
                addFields(
                    TRIGGER_FIELD to "true",
                    HEADER to hashMapOf(
                        BEGIN_STRING to "FIXT.1.1",
                        MSG_SEQ_NUM to System.currentTimeMillis(),
                        MSG_TYPE to "D",
                    ),
                    TRANSACT_TIME to Instant.now(),
                    CL_ORD_ID to System.currentTimeMillis().toString(),
                    ORDER_QTY to Random.nextInt(1, 100),
                    PRICE to Random.nextDouble(0.1, 100.0),
                )
                func()
            }.build()

        private fun buildMessage(side: String, securityId: String) = buildMessage {
            addField(SIDE, side)
            addField(SECURITY_ID, securityId)
        }

        private fun expectNewBuyIsPlaced(
            nos: ParsedMessage,
            er: ParsedMessage.FromMapBuilder,
            alias: String,
            orderId: Int,
            execId: Int,
        ) {
            expectThat(er) {
                get { idBuilder() } and {
                    get { sessionAlias } isEqualTo alias
                }
                get { type } isEqualTo EXECUTION_REPORT
                get { bodyBuilder() } and {
                    get { size } isEqualTo 13
                    (nos.body - NOT_COPIED_FIELD).forEach { (key, value) ->
                        get(key) { get(key).toString() } isEqualTo value.toString()
                    }
                    get { get(TRANSACT_TIME) }.isNotNull()
                    get { get(ORDER_ID) } isEqualTo orderId
                    get { get(LEAVES_QTY) } isEqualTo nos.body[ORDER_QTY]
                    get { get(TEXT) } isEqualTo "Simulated New Order Buy is placed"
                    get { get(EXEC_TYPE) } isEqualTo EXEC_TYPE_NEW
                    get { get(ORD_STATUS) } isEqualTo ORD_STATUS_NEW
                    get { get(CUM_QTY) } isEqualTo "0"
                    get { get(EXEC_ID) } isEqualTo execId
                }
            }
        }

        private fun expectRejected(
            nos: ParsedMessage,
            r: ParsedMessage.FromMapBuilder,
            refTagId: Int,
        ) {
            expectThat(r) {
                get { idBuilder() } and {
                    get { sessionAlias } isEqualTo nos.id.sessionAlias
                }
                get { type } isEqualTo REJECT
                get { bodyBuilder() } and {
                    get { size } isEqualTo 5
                    get { get(REF_TAG_ID) } isEqualTo refTagId.toString()
                    get { get(REF_MSG_TYPE) } isEqualTo "D"
                    get { get(REF_SEQ_NUM) } isEqualTo (nos.body[HEADER] as Map<*, *>)[MSG_SEQ_NUM]
                    get { get(TEXT) } isEqualTo "Simulating reject message"
                    get { get(SESSION_REJECT_REASON) } isEqualTo SESSION_REJECT_REASON_REQUIRED_TAG_MISSING
                }
            }
        }

        private fun expectBMRejected(
            nos: ParsedMessage,
            r: ParsedMessage.FromMapBuilder,
        ) {
            expectThat(r) {
                get { idBuilder() } and {
                    get { sessionAlias } isEqualTo nos.id.sessionAlias
                }
                get { type } isEqualTo BUSINESS_MESSAGE_REJECT
                get { bodyBuilder() } and {
                    get { size } isEqualTo 6
                    get { get(REF_TAG_ID) } isEqualTo "48"
                    get { get(REF_MSG_TYPE) } isEqualTo "D"
                    get { get(REF_SEQ_NUM) } isEqualTo (nos.body[HEADER] as Map<*, *>)[MSG_SEQ_NUM]
                    get { get(TEXT) } isEqualTo "Unknown SecurityID"
                    get { get(BUSINESS_REJECT_REASON) } isEqualTo BUSINESS_REJECT_REASON_UNKNOWN_SECURITY
                    get { get(BUSINESS_REJECT_REF_ID) } isEqualTo nos.body[CL_ORD_ID]
                }
            }
        }

        @Suppress("SameParameterValue")
        private fun expectOrderExpired(
            sell: ParsedMessage,
            buy1: ParsedMessage?,
            buy2: ParsedMessage?,
            er: ParsedMessage.FromMapBuilder,
            alias: String,
            orderId: Int,
            execId: Int,
        ) {
            expectThat(er) {
                get { idBuilder() } and {
                    get { sessionAlias } isEqualTo alias
                }
                get { type } isEqualTo EXECUTION_REPORT
                get { bodyBuilder() } and {
                    get { size } isEqualTo 13
                    (sell.body - NOT_COPIED_FIELD).forEach { (key, value) ->
                        get(key) { get(key).toString() } isEqualTo value.toString()
                    }
                    get { get(TRANSACT_TIME) }.isNotNull()
                    get { get(ORDER_ID) } isEqualTo orderId
                    get { get(LEAVES_QTY) } isEqualTo "0"
                    get { get(TEXT) } isEqualTo "The remaining part of simulated order has been expired"
                    get { get(EXEC_TYPE) } isEqualTo EXEC_TYPE_EXPIRED
                    get { get(ORD_STATUS) } isEqualTo ORD_STATUS_EXPIRED
                    get { get(CUM_QTY) } isEqualTo (
                            (buy1?.body[ORDER_QTY]?.toString()?.toInt() ?: 0)
                                    + (buy2?.body[ORDER_QTY]?.toString()?.toInt() ?: 0)
                            )
                    get { get(EXEC_ID) } isEqualTo execId
                }
            }
        }

        @Suppress("SameParameterValue")
        private fun expectExtraER(
            sell: ParsedMessage,
            buy1: ParsedMessage,
            buy2: ParsedMessage,
            er: ParsedMessage.FromMapBuilder,
            alias: String,
            orderId: Int,
            execId: Int,
            matchId: Int,
        ) {
            expectThat(er) {
                get { idBuilder() } and {
                    get { sessionAlias } isEqualTo alias
                }
                get { type } isEqualTo EXECUTION_REPORT
                get { bodyBuilder() } and {
                    get { size } isEqualTo 17
                    (sell.body - NOT_COPIED_FIELD).forEach { (key, value) ->
                        get(key) { get(key).toString() } isEqualTo value.toString()
                    }
                    get { get(TRANSACT_TIME) }.isNotNull()
                    get { get(ORDER_ID) } isEqualTo orderId
                    get { get(LEAVES_QTY) } isEqualTo (
                            sell.body[ORDER_QTY].toString().toInt()
                                    - buy1.body[ORDER_QTY].toString().toInt()
                                    - buy2.body[ORDER_QTY].toString().toInt()
                            )
                    get { get(TEXT) } isEqualTo "Extra Execution Report"
                    get { get(EXEC_TYPE) } isEqualTo EXEC_TYPE_TRADE
                    get { get(ORD_STATUS) } isEqualTo ORD_STATUS_FILLED
                    get { get(CUM_QTY) } isEqualTo buy1.body[ORDER_QTY].toString()
                        .toInt() + buy2.body[ORDER_QTY].toString().toInt()
                    get { get(EXEC_ID) } isEqualTo execId
                    get { get(LAST_PX) } isEqualTo buy1.body[PRICE].toString()
                    get { get(TRD_MATCH_ID) } isEqualTo matchId
                    get { get(AGGRESSOR_INDICATOR) } isEqualTo "Y"
                    extractTradingParty("DEMO-CONN2", "DEMOFIRM1")
                }
            }
        }

        private fun expectOrderFullFilled(
            nos: ParsedMessage,
            er: ParsedMessage.FromMapBuilder,
            alias: String,
            orderId: Int,
            execId: Int,
            matchId: Int,
        ) {
            expectThat(er) {
                get { idBuilder() } and {
                    get { sessionAlias } isEqualTo alias
                }
                get { type } isEqualTo EXECUTION_REPORT
                get { bodyBuilder() } and {
                    get { size } isEqualTo 18
                    (nos.body - NOT_COPIED_FIELD).forEach { (key, value) ->
                        get(key) { get(key).toString() } isEqualTo value.toString()
                    }
                    get { get(TRANSACT_TIME) }.isNotNull()
                    get { get(ORDER_ID) } isEqualTo orderId
                    get { get(TRD_MATCH_ID) } isEqualTo matchId
                    get { get(LEAVES_QTY) } isEqualTo 0
                    get { get(TEXT) } isEqualTo "The simulated order has been fully traded"
                    get { get(EXEC_TYPE) } isEqualTo EXEC_TYPE_TRADE
                    get { get(ORD_STATUS) } isEqualTo ORD_STATUS_FILLED
                    get { get(LAST_PX).toString() } isEqualTo nos.body[PRICE].toString()
                    get { get(CUM_QTY) } isEqualTo nos.body[ORDER_QTY].toString().toInt()
                    get { get(EXEC_ID) } isEqualTo execId
                    get { get(TIME_IN_FORCE) } isEqualTo TIME_IN_FORCE_DAY
                    get { get(AGGRESSOR_INDICATOR) } isEqualTo "N"
                    extractTradingParty("DEMO-CONN1", "DEMOFIRM2")
                }
            }
        }

        private fun Assertion.Builder<MapBuilder<String, Any?>>.extractTradingParty(
            connect: String,
            firm: String
        ) {
            get { get(TRADING_PARTY) }.isA<Map<*, *>>() and {
                get { size } isEqualTo 1
                get { get(NO_PARTY_IDS) }.isA<List<*>>() and {
                    get { size } isEqualTo 5
                    get { get(0) }.isA<Map<*, *>>() and {
                        get { size } isEqualTo 3
                        get { get(PARTY_ROLE) } isEqualTo PARTY_ROLE_DECK_ID
                        get { get(PARTY_ID) } isEqualTo connect
                        get { get(PARTY_ID_SOURCE) } isEqualTo PARTY_ID_SOURCE_PROPRIETARY_CUSTOM_CODE
                    }
                    get { get(1) }.isA<Map<*, *>>() and {
                        get { size } isEqualTo 3
                        get { get(PARTY_ROLE) } isEqualTo PARTY_ROLE_CONTRA_FIRM
                        get { get(PARTY_ID) } isEqualTo firm
                        get { get(PARTY_ID_SOURCE) } isEqualTo PARTY_ID_SOURCE_PROPRIETARY_CUSTOM_CODE
                    }
                    get { get(2) }.isA<Map<*, *>>() and {
                        get { size } isEqualTo 3
                        get { get(PARTY_ROLE) } isEqualTo PARTY_ROLE_CLIENT_ID
                        get { get(PARTY_ID) } isEqualTo "0"
                        get { get(PARTY_ID_SOURCE) } isEqualTo PARTY_ID_SOURCE_SHORT_CODE_IDENTIFIER
                    }
                    get { get(3) }.isA<Map<*, *>>() and {
                        get { size } isEqualTo 3
                        get { get(PARTY_ROLE) } isEqualTo PARTY_ROLE_INVESTMENT_DIVISION_MARKER
                        get { get(PARTY_ID) } isEqualTo "0"
                        get { get(PARTY_ID_SOURCE) } isEqualTo PARTY_ID_SOURCE_SHORT_CODE_IDENTIFIER
                    }
                    get { get(4) }.isA<Map<*, *>>() and {
                        get { size } isEqualTo 3
                        get { get(PARTY_ROLE) } isEqualTo PARTY_ROLE_EXECUTING_TRADER
                        get { get(PARTY_ID) } isEqualTo "3"
                        get { get(PARTY_ID_SOURCE) } isEqualTo PARTY_ID_SOURCE_SHORT_CODE_IDENTIFIER
                    }
                }
            }
        }

        @Suppress("SameParameterValue")
        private fun expectOrderPartiallyTraded(
            sell: ParsedMessage,
            buy1: ParsedMessage,
            buy2: ParsedMessage?,
            er: ParsedMessage.FromMapBuilder,
            alias: String,
            orderId: Int,
            execId: Int,
            matchId: Int,
        ) {
            expectThat(er) {
                get { idBuilder() } and {
                    get { sessionAlias } isEqualTo alias
                }
                get { type } isEqualTo EXECUTION_REPORT
                get { bodyBuilder() } and {
                    get { size } isEqualTo 17
                    (sell.body - NOT_COPIED_FIELD).forEach { (key, value) ->
                        get(key) { get(key).toString() } isEqualTo value.toString()
                    }
                    get { get(TRANSACT_TIME) }.isNotNull()
                    get { get(ORDER_ID) } isEqualTo orderId
                    get { get(TRD_MATCH_ID) } isEqualTo matchId
                    get { get(LEAVES_QTY) } isEqualTo (sell.body[ORDER_QTY] as Int) - (buy1.body[ORDER_QTY] as Int) - (buy2?.body[ORDER_QTY]?.toString()
                        ?.toInt() ?: 0)
                    get { get(TEXT) } isEqualTo "The simulated order has been partially traded"
                    get { get(EXEC_TYPE) } isEqualTo EXEC_TYPE_TRADE
                    get { get(ORD_STATUS) } isEqualTo ORD_STATUS_PARTIALLY_FILLED
                    get { get(LAST_PX).toString() } isEqualTo buy1.body[PRICE].toString()
                    get { get(CUM_QTY) } isEqualTo buy1.body[ORDER_QTY].toString()
                        .toInt() + (buy2?.body[ORDER_QTY]?.toString()?.toInt() ?: 0)
                    get { get(EXEC_ID) } isEqualTo execId
                    get { get(AGGRESSOR_INDICATOR) } isEqualTo "Y"
                    extractTradingParty("DEMO-CONN2", "DEMOFIRM1")
                }
            }
        }

        @Suppress("SameParameterValue")
        private fun expectIncorrectValueInOrdStsTag(
            sell: ParsedMessage,
            buy1: ParsedMessage,
            buy2: ParsedMessage?,
            er: ParsedMessage.FromMapBuilder,
            alias: String,
            orderId: Int,
            execId: Int,
            matchId: Int,
        ) {
            expectThat(er) {
                get { idBuilder() } and {
                    get { sessionAlias } isEqualTo alias
                }
                get { type } isEqualTo EXECUTION_REPORT
                get { bodyBuilder() } and {
                    get { size } isEqualTo 19
                    (sell.body - NOT_COPIED_FIELD).forEach { (key, value) ->
                        get(key) { get(key).toString() } isEqualTo value.toString()
                    }
                    get { get(TRANSACT_TIME) }.isNotNull()
                    get { get(ORDER_ID) } isEqualTo orderId
                    get { get(TRD_MATCH_ID) } isEqualTo matchId
                    get { get(LEAVES_QTY) } isEqualTo (sell.body[ORDER_QTY] as Int) - (buy1.body[ORDER_QTY] as Int) - (buy2?.body[ORDER_QTY]?.toString()
                        ?.toInt() ?: 0)
                    get { get(TEXT) } isEqualTo "Execution Report with incorrect value in OrdStatus tag"
                    get { get(EXEC_TYPE) } isEqualTo EXEC_TYPE_TRADE
                    get { get(ORD_STATUS) } isEqualTo ORD_STATUS_PARTIALLY_FILLED
                    get { get(LAST_PX).toString() } isEqualTo buy1.body[PRICE].toString()
                    get { get(CUM_QTY) } isEqualTo (
                            buy1.body[ORDER_QTY].toString().toInt() + (buy2?.body[ORDER_QTY]?.toString()?.toInt() ?: 0)
                            )
                    get { get(EXEC_ID) } isEqualTo execId
                    get { get(ORDER_CAPACITY) } isEqualTo ORDER_CAPACITY_PRINCIPAL
                    get { get(ACCOUNT_TYPE) } isEqualTo ACCOUNT_TYPE_NON_CUSTOMER
                    get { get(AGGRESSOR_INDICATOR) } isEqualTo "Y"
                    extractTradingParty("DEMO-CONN2", "DEMOFIRM1")
                }
            }
        }

        @Suppress("SameParameterValue")
        private fun expectCsvFiles(
            dir: Path,
            filesToContent: List<Pair<Path, String>>,
            pattern: String,
            checkpoint: Instant,
        ) {
            assertAll(
                {
                    assertAll(
                        filesToContent.map(Pair<Path, String>::first).dropLast(1).map { path ->
                            { assertFalse(path.exists(), path.absolutePathString()) }
                        }
                    )
                },
                {
                    val (file, content) = filesToContent.last()
                    val files = dir.listDirectoryEntries().toList()
                    assertAll(
                        { assertEquals(2, files.size, "check size of $files") },
                        { assertEquals(content, files.single { it == file }.readText(), "check content of $file") },
                        {
                            val newFile = files.single { it != file }
                            assertAll(
                                { assertTrue(newFile.name.startsWith(pattern), "check pattern in $newFile") },
                                { assertEquals("csv", newFile.extension, "check extension in $newFile") },
                                {
                                    val lines = CSVReader(FileReader(newFile.toFile())).use(CSVReader::readAll)
                                    expectThat(lines) {
                                        get { size } isEqualTo 2
                                        get { get(0) } and {
                                            get { size } isEqualTo 8
                                            get { get(0) } isEqualTo "Action"
                                            get { get(1) } isEqualTo "TransactTime"
                                            get { get(2) } isEqualTo "ClOrdID"
                                            get { get(3) } isEqualTo "OrdID"
                                            get { get(4) } isEqualTo "Instrument"
                                            get { get(5) } isEqualTo "Side"
                                            get { get(6) } isEqualTo "Price"
                                            get { get(7) } isEqualTo "Qty"
                                        }
                                        get { get(1) } and {
                                            get { size } isEqualTo 8
                                            get { get(0) } isEqualTo "FULL_RESET"
                                            get { get(1) }.isNotNull() and {
                                                get { Instant.parse(this) }.isGreaterThan(checkpoint)
                                            }
                                            get { get(2) }.isBlank()
                                            get { get(3) }.isBlank()
                                            get { get(4) }.isBlank()
                                            get { get(5) }.isBlank()
                                            get { get(6) }.isBlank()
                                            get { get(7) }.isBlank()
                                        }
                                    }
                                }
                            )
                        },
                        { assertTrue(files.all { it.name.startsWith(pattern) }, "check pattern in $files") },
                        { assertTrue(files.all { it.extension == "csv" }, "check extension in $files") },
                    )
                }
            )
        }
    }
}