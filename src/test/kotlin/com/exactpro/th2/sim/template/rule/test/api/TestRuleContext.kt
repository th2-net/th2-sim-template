package com.exactpro.th2.sim.template.rule.test.api

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventOrBuilder
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.sim.rule.IRule
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.action.IAction
import com.exactpro.th2.sim.rule.action.ICancellable
import com.exactpro.th2.sim.rule.action.impl.ActionRunner
import com.exactpro.th2.sim.rule.action.impl.MessageSender
import com.google.protobuf.GeneratedMessageV3
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.fail
import org.opentest4j.AssertionFailedError
import java.util.Deque
import java.util.LinkedList
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class TestRuleContext : IRuleContext {

    private val messageSender = MessageSender(this::send, this::send)

    private val cancellables: Deque<ICancellable> = ConcurrentLinkedDeque()
    private val scheduledExecutorService: ScheduledExecutorService = Executors.newScheduledThreadPool(5)

    private val results: Queue<GeneratedMessageV3> = LinkedList()

    override fun send(msg: Message) {
        results.add(msg)
        logger.trace { "Message sent: $msg" }
    }

    override fun send(batch: MessageBatch) {
        results.add(batch)
        logger.trace { "Batch sent: $batch" }
    }

    override fun send(msg: Message, delay: Long, timeUnit: TimeUnit) {
        scheduledExecutorService.schedule({
            send(msg)
        }, delay, timeUnit)
    }

    override fun send(batch: MessageBatch, delay: Long, timeUnit: TimeUnit) {
        scheduledExecutorService.schedule({
            send(batch)
        }, delay, timeUnit)
    }

    override fun execute(action: IAction): ICancellable = registerCancellable(ActionRunner(scheduledExecutorService,  messageSender, action))

    override fun execute(delay: Long, action: IAction): ICancellable = registerCancellable(ActionRunner(scheduledExecutorService, messageSender, delay, action))

    override fun execute(delay: Long, period: Long, action: IAction): ICancellable  = registerCancellable(ActionRunner(scheduledExecutorService, messageSender, delay, period, action))

    override fun getRootEventId(): String {
        return "testEventID"
    }

    override fun sendEvent(event: com.exactpro.th2.common.event.Event) {
        results.add(event.toProtoEvent(rootEventId))
        logger.trace { "Event sent: $event" }
    }

    override fun removeRule() {
        cancellables.forEach { cancellable ->
            runCatching(cancellable::cancel).onFailure {
                logger.error(it) { "Failed to cancel sub-task of rule" }
            }
        }
        logger.trace { "Rule removed" }
    }

    fun resetResults() {
        results.clear()
    }

    private fun registerCancellable(cancellable: ICancellable): ICancellable {
        cancellables.add(cancellable)
        return cancellable
    }

    fun IRule.assertNotTriggered(testMessage: Message, lazyMessage: () -> String? = {null}) {
        if (checkTriggered(testMessage)) {
            fail { "${buildPrefix(lazyMessage())}Rule ${this.javaClass.simpleName} expected: <not triggered> but was: <triggered>" }
        }
        logger.trace { "Rule ${this.javaClass.name} was successfully not triggered" }
    }

    fun IRule.assertTriggered(testMessage: Message, lazyMessage: () -> String? = {null}) {
        if (!checkTriggered(testMessage)) {
            fail { "${buildPrefix(lazyMessage())}Rule ${this.javaClass.simpleName} expected: <triggered> but was: <not triggered>" }
        }
        handle(this@TestRuleContext, testMessage)
        logger.trace { "Rule ${this.javaClass.name} was successfully triggered" }
    }

    fun assertNothingSent(lazyMessage: () -> String? = {null}) {
        val actual = results.peek()
        if (actual!=null) {
            fail { "${buildPrefix(lazyMessage())}Rule ${this.javaClass.simpleName} expected: <Nothing> but was: <${if (actual is MessageBatch) "MessageBatch" else "Event"}>" }
        }
        logger.trace { "Rule ${this.javaClass.name}: successfully nothing was sent" }
    }

    fun assertSent(expected: GeneratedMessageV3, lazyMessage: () -> String? = {null}) {
        when (expected) {
            is Message -> assertSent { actual: Message ->
                assertEqualsMessages(expected, actual, lazyMessage)
            }
            is MessageBatch -> assertSent { actual: MessageBatch ->
                assertEqualsBatches(expected, actual, lazyMessage)
            }
            is Event -> assertSent { actual: Event ->
                Assertions.assertEquals(expected, actual, lazyMessage)
            }
        }
    }

    @JvmName("assertSentMessage")
    fun assertSent(testCase: (Message) -> Unit) {
        val actual = results.peek()
        Assertions.assertNotNull(actual) {"Nothing was sent from rule"}

        if (actual !is Message) {
            fail { "Rule ${this.javaClass.simpleName} expected: <Message> but was: <${if (actual is MessageBatch) "MessageBatch" else "Event"}>" }
        }

        testCase(actual)

        logger.trace { "Rule ${this.javaClass.name}: Message was successfully sent" }
        results.poll()
    }

    @JvmName("assertSentMessageBatch")
    fun assertSent(testCase: (MessageBatch) -> Unit) {
        val actual = results.peek()
        Assertions.assertNotNull(actual) {"Nothing was sent from rule"}

        if (actual !is MessageBatch) {
            fail { "Rule ${this.javaClass.simpleName} expected: <MessageBatch> but was: <${if (actual is Message) "Message" else "Event"}>" }
        }

        testCase(actual)

        logger.trace { "Rule ${this.javaClass.name}: MessageBatch was successfully sent" }
        results.poll()
    }

    @JvmName("assertSentEvent")
    fun assertSent(testCase: (Event) -> Unit) {
        val actual = results.peek()
        Assertions.assertNotNull(actual) {"Nothing was sent from rule"}

        if (actual !is Event) {
            fail { "Rule ${this.javaClass.simpleName} expected: <Event> but was: <${if (actual is MessageBatch) "MessageBatch" else "Message"}>" }
        }

        testCase(actual)

        logger.trace { "Rule ${this.javaClass.name}: Event was successfully sent" }
        results.poll()
    }


    inline fun <R> test(block: TestRuleContext.() -> R): R = block().apply {
        this@TestRuleContext.removeRule()
        this@TestRuleContext.resetResults()
    }

    companion object {
        private val logger = KotlinLogging.logger {}
    }
}