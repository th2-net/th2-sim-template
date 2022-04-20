package com.exactpro.th2.sim.template.rule

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.addFields
import com.exactpro.th2.common.message.copyFields
import com.exactpro.th2.common.message.get
import com.exactpro.th2.common.message.message
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule
import com.exactpro.th2.sim.template.rule.NOSRule.Companion
import java.time.LocalDateTime

class CancelRule(field: Map<String, Value>) : MessageCompareRule() {

    init {
        init("OrderCancelRequest", field)
    }

    override fun handle(ruleContext: IRuleContext, incomeMessage: Message) {
        val ordID = incomeMessage["OrderID"]?.simpleValue
        if (NOSRule.orders.containsKey(ordID) && NOSRule.orders[ordID] != null) {
            val fixCancel = NOSRule.orders[ordID]
            fixCancel!!.copyFields(
                incomeMessage,
                "ClOrdID"
            ).addFields(
                "TransactTime", LocalDateTime.now(),
                "Text", "Simulated order is cancelled",
                "ExecType", "4",
                "OrdStatus", "4",
                "CumQty", "0",
                "LeavesQty", "0"
            )
            ruleContext.send(fixCancel.build())
            NOSRule.orders.remove(ordID)
            println(NOSRule.orders.keys.toString() + " removed " + ordID + ". Total:" + Companion.orders.size)
        } else {
            val fixReject = message("ExecutionReport")
                .copyFields(
                    incomeMessage,
                    "OrderID",
                    "ClOrdID",
                    "SecurityID",
                    "SecurityIDSource",
                    "TradingParty",
                )
                .addFields(
                    "OrdRejReason", "5",
                    "ExecType", "8",
                    "LeavesQty", "0",
                    "CumQty", "0",
                    "ExecID", "0",
                    "OrdStatus", "0",
                    "RejectText", "Unknown order"
                )
            ruleContext.send(fixReject.build())
        }
    }
}