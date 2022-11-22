package com.exactpro.th2.sim.template.rule

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.addFields
import com.exactpro.th2.common.message.copyFields
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.message.getMessage
import com.exactpro.th2.common.value.getString
import com.exactpro.th2.common.message.message
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger

class CustomNOSRule(field: Map<String, Value>, textSuffix1: String, textSuffix2: String) : MessageCompareRule() {
    private var innerTextSuffix1: String
    private var innerTextSuffix2: String

    companion object {
        private val orderID = AtomicInteger(0)
        private val execID = AtomicInteger(0)
        val creationTime: LocalDateTime = LocalDateTime.now()
    }

    init {
        init("NewOrderSingle", field)
        this.innerTextSuffix1 = textSuffix1
        this.innerTextSuffix2 = textSuffix2
    }

    override fun handle(ruleContext: IRuleContext, incomeMessage: Message) {
        val fixNew = message("ExecutionReport")
                .copyFields(
                        incomeMessage,
                        "Side",
                        "Price",
                        "ClOrdID",
                        "OrdType",
                        "TimeInForce",
                        "OrderCapacity",
                        "AccountType"
                ).addFields(
                        "TransactTime", LocalDateTime.now(),
                        "OrderID", orderID.incrementAndGet(),
                        "ExecID", execID.incrementAndGet(),
                        "LeavesQty", incomeMessage.getField("OrderQty")!!,
                        "Text", "$creationTime / ${this.innerTextSuffix1} / ${this.innerTextSuffix2}",
                        "ExecType", "0",
                        "OrdStatus", "0",
                        "CumQty", "0"
                )
        ruleContext.send(fixNew.build())
    }

    override fun touch(ruleContext: IRuleContext, args: MutableMap<String, String>) {
        super.touch(ruleContext, args)
        this.innerTextSuffix1 = args.getOrDefault("textSuffix1", this.innerTextSuffix1)
        this.innerTextSuffix2 = args.getOrDefault("textSuffix2", this.innerTextSuffix2)
    }
}
