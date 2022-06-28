package com.exactpro.th2.sim.template.rule

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.addFields
import com.exactpro.th2.common.message.copy
import com.exactpro.th2.common.message.copyFields
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.value.getString
import com.exactpro.th2.common.message.message
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger

class NOSRule(field: Map<String, Value>) : MessageCompareRule() {

    companion object {
        private val orderID = AtomicInteger(0)
        private val execID = AtomicInteger(0)
        val orders = hashMapOf<String, Message.Builder>()
    }

    init {
        init("NewOrderSingle", field)
    }

    override fun handle(ruleContext: IRuleContext, incomeMessage: Message) {

        val security_ids_ignore = arrayOf("INSTR1", "INSTR2", "INSTR3", "INSTR4", "INSTR5", "INSTR6")

        if (incomeMessage.getField("SecurityID")!!.getString() !in security_ids_ignore) {
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
                            "OrderID", orderID.incrementAndGet(),
                            "ExecID", execID.incrementAndGet(),
                            "LeavesQty", incomeMessage.getField("OrderQty")!!,
                            "Text", "Simulated order is placed",
                            "ExecType", "0",
                            "OrdStatus", "0",
                            "CumQty", "0"
                    )

            orders[orderID.toString()] = fixNew.copy()
            println(orders.keys.toString() + " added " + orderID + ". Total:" + orders.size)
            ruleContext.send(fixNew.build())
        }
    }
}