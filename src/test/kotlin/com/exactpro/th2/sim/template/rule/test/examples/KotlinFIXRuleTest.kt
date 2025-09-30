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

package com.exactpro.th2.sim.template.rule.test.examples

import com.exactpro.th2.common.annotations.IntegrationTest
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.toBatch
import com.exactpro.th2.common.utils.message.transport.toGroup
import com.exactpro.th2.sim.configuration.SimulatorConfiguration
import com.exactpro.th2.sim.grpc.SimService
import com.exactpro.th2.sim.impl.Simulator
import com.exactpro.th2.sim.impl.SimulatorServer
import com.exactpro.th2.sim.template.FixFields
import com.exactpro.th2.sim.template.service.TemplateService
import com.exactpro.th2.test.annotations.Th2AppFactory
import com.exactpro.th2.test.annotations.Th2IntegrationTest
import com.exactpro.th2.test.annotations.Th2TestFactory
import com.exactpro.th2.test.extension.CleanupExtension
import com.exactpro.th2.test.spec.CustomConfigSpec
import com.exactpro.th2.test.spec.GrpcSpec
import com.exactpro.th2.test.spec.RabbitMqSpec
import com.exactpro.th2.test.spec.filter
import com.exactpro.th2.test.spec.metadata
import com.exactpro.th2.test.spec.pin
import com.exactpro.th2.test.spec.pins
import com.exactpro.th2.test.spec.publishers
import com.exactpro.th2.test.spec.server
import com.exactpro.th2.test.spec.subscribers
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals


@Th2IntegrationTest
@IntegrationTest
internal class KotlinFIXRuleTest {

    @Suppress("unused")
    val mq = RabbitMqSpec.create()
        .pins {
            publishers {
                pin("out") {
                    attributes("transport-group")
                    filter {
                        metadata {
                            sessionAlias().shouldMatchWildcard("*-fix-*")
                        }
                    }
                }
            }
            subscribers {
                pin("in") {
                    attributes("transport-group")
                }
            }
        }

    @Suppress("unused")
    val grpc = GrpcSpec.create()
        .server<SimService>()
        .server<TemplateService>()

    @Suppress("unused")
    val custom = CustomConfigSpec.fromObject(
        MAPPER.readValue(
            """
        defaultRules:
          - methodName: createDemoRule
            enable: true
            settings:
              connection_id:
                session_alias: $TEST_SESSION_ALIAS
              session_aliases:
                ALIAS_1: $TEST_SESSION_ALIAS
    """.trimIndent(), SimulatorConfiguration::class.java
        )
    )

    @Test
    fun `reject response`(
        @Th2AppFactory factory: CommonFactory,
        @Th2TestFactory test: CommonFactory,
        resourceCleaner: CleanupExtension.Registry,
    ) {
        resourceCleaner.add(createSimServer(factory))
        val messages = mutableListOf<ParsedMessage>()
        test.transportGroupBatchRouter.subscribe({ _, batch ->
            batch.groups.asSequence()
                .flatMap { it.messages.asSequence() }
                .forEach {
                    check(it is ParsedMessage) { "Incorrect type of message ${it::class.simpleName}" }
                    messages.add(it)
                }
        })

        test.transportGroupBatchRouter.send(
            ParsedMessage.builder()
                .setId(createMessageId())
                .setType("NewOrderSingle")
                .setBody(emptyMap())
                .build().toGroup().toBatch(TEST_BOOK, TEST_SESSION_GROUP)
        )

        await("responded messages").atMost(5, TimeUnit.SECONDS).until { messages.isNotEmpty() }

        assertEquals("Reject", messages.single().type) // FIXME: create full check
    }

    @Test
    fun `business message reject response`(
        @Th2AppFactory factory: CommonFactory,
        @Th2TestFactory test: CommonFactory,
        resourceCleaner: CleanupExtension.Registry,
    ) {
        resourceCleaner.add(createSimServer(factory))
        val messages = mutableListOf<ParsedMessage>()
        test.transportGroupBatchRouter.subscribe({ _, batch ->
            batch.groups.asSequence()
                .flatMap { it.messages.asSequence() }
                .forEach {
                    check(it is ParsedMessage) { "Incorrect type of message ${it::class.simpleName}" }
                    messages.add(it)
                }
        })

        test.transportGroupBatchRouter.send(
            ParsedMessage.builder()
                .setId(createMessageId())
                .setType("NewOrderSingle")
                .setBody(mapOf(
                    FixFields.SIDE to 1,
                    FixFields.SECURITY_ID to "INSTR6",
                ))
                .build().toGroup().toBatch(TEST_BOOK, TEST_SESSION_GROUP)
        )

        await("responded messages").atMost(5, TimeUnit.SECONDS).until { messages.isNotEmpty() }

        assertEquals("BusinessMessageReject", messages.single().type) // FIXME: create full check
    }

    @Test
    fun `execution report response`(
        @Th2AppFactory factory: CommonFactory,
        @Th2TestFactory test: CommonFactory,
        resourceCleaner: CleanupExtension.Registry,
    ) {
        resourceCleaner.add(createSimServer(factory))
        val messages = mutableListOf<ParsedMessage>()
        test.transportGroupBatchRouter.subscribe({ _, batch ->
            batch.groups.asSequence()
                .flatMap { it.messages.asSequence() }
                .forEach {
                    check(it is ParsedMessage) { "Incorrect type of message ${it::class.simpleName}" }
                    messages.add(it)
                }
        })

        test.transportGroupBatchRouter.send(
            ParsedMessage.builder()
                .setId(createMessageId())
                .setType("NewOrderSingle")
                .setBody(
                    mapOf(
                        FixFields.SIDE to 1,
                        FixFields.PRICE to 1.2,
                        FixFields.CUM_QTY to 34,
                        FixFields.CL_ORD_ID to "test-cl-ord-id-${System.currentTimeMillis()}",
                        FixFields.SECONDARY_CL_ORD_ID to "test-secondary-cl-ord-id-${System.currentTimeMillis()}",
                        FixFields.SECURITY_ID to "INSTR5",
                        FixFields.SECURITY_ID_SOURCE to "test-security-id-source",
                        FixFields.ORD_TYPE to "test-order-type",
                        FixFields.ORDER_QTY to 56,
                        FixFields.TRADING_PARTY to "test-trading-party",
                        FixFields.TIME_IN_FORCE to "IOC",
                        FixFields.ORDER_CAPACITY to "test-order-capacity",
                        FixFields.ACCOUNT_TYPE to "test-account-type",
                    )
                ).build().toGroup().toBatch(TEST_BOOK, TEST_SESSION_GROUP)
        )

        await("responded messages").atMost(5, TimeUnit.SECONDS).until { messages.isNotEmpty() }

        assertEquals("ExecutionReport", messages[0].type) // FIXME: create full check
        assertEquals("ExecutionReport", messages[1].type) // FIXME: create full check
    }

    companion object {
        private const val TEST_BOOK = "test-book"
        private const val TEST_SESSION_GROUP = "test-session-fix-group"
        private const val TEST_SESSION_ALIAS = "test-session-fix-alias"

        private val MAPPER: ObjectMapper = ObjectMapper(YAMLFactory())

        private fun createSimServer(factory: CommonFactory) = SimulatorServer().apply {
            init(factory, Simulator::class.java)
            start()
        }

        private fun createMessageId(
            alias: String = TEST_SESSION_ALIAS,
            timestamp: Instant = Instant.now(),
            direction: Direction = Direction.INCOMING,
            sequence: Long = System.currentTimeMillis()
        ) = MessageId.builder()
            .setSessionAlias(alias)
            .setTimestamp(timestamp)
            .setDirection(direction)
            .setSequence(sequence)
            .build()
    }
}