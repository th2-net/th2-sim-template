package com.exactpro.th2.sim.template.rule.test.api

import com.exactpro.th2.common.event.Event
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

    private val results: Queue<GeneratedMessageV3> = LinkedList<GeneratedMessageV3>()

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

    override fun sendEvent(event: Event) {
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

    fun IRule.assertNotTriggered(message: Message) {
        Assertions.assertFalse(checkTriggered(message)) {"Rule ${this.javaClass.name} shouldn't have triggered"}
        logger.trace { "Rule ${this.javaClass.name} was successfully not triggered" }
    }

    fun IRule.assertTriggered(message: Message) {
        Assertions.assertTrue(checkTriggered(message)) {"Rule ${this.javaClass.name} should have triggered"}
        handle(this@TestRuleContext, message)
        run {  }
        logger.trace { "Rule ${this.javaClass.name} was successfully triggered" }
    }

    inline fun <R> test(block: TestRuleContext.() -> R): R {
        return block().apply {
            this@TestRuleContext.removeRule()
            this@TestRuleContext.resetResults()
        }
    }

    companion object {
        private val logger = KotlinLogging.logger {}
    }
}