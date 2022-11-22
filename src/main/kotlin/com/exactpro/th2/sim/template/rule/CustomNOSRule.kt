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

class CustomNOSRule(field: Map<String, Value>, val textSuffix1: String, val textSuffix2: String) : MessageCompareRule() {

    companion object {
        private val orderID = AtomicInteger(0)
        private val execID = AtomicInteger(0)
        val creationTime: LocalDateTime = LocalDateTime.now()
    }

    init {
        init("NewOrderSingle", field)
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
                        "Text", "$creationTime / $textSuffix1 / $textSuffix2",
                        "ExecType", "0",
                        "OrdStatus", "0",
                        "CumQty", "0"
                )
        ruleContext.send(fixNew.build())
    }
}
