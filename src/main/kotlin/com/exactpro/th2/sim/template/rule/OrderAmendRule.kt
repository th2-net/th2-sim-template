package com.exactpro.th2.sim.template.rule

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.addFields
import com.exactpro.th2.common.message.copyFields
import com.exactpro.th2.common.message.get
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.message.message
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule
import com.exactpro.th2.sim.template.rule.OrderNewRule.Companion
import java.time.LocalDateTime

class OrderAmendRule(field: Map<String, Value>) : MessageCompareRule() {

    init {
        init("OrderCancelReplaceRequest", field)
    }

    override fun handle(ruleContext: IRuleContext, incomeMessage: Message) {
        val ordID = incomeMessage["OrderID"]?.simpleValue
        if (OrderNewRule.orders.containsKey(ordID) && OrderNewRule.orders[ordID] != null) {
            val fixAmend = OrderNewRule.orders[ordID]
            fixAmend!!.copyFields(
                incomeMessage,
                "Side",
                "Price",
                "ClOrdID",
                "SecurityID",
                "SecurityIDSource",
                "OrdType",
                "OrderQty",
                "TimeInForce",
                "OrderCapacity",
                "AccountType"
            ).addFields(
                "LeavesQty", incomeMessage.getField("OrderQty")!!,
                "TransactTime", LocalDateTime.now(),
                "Text", "Simulated order is amended",
                "ExecType", "5",
                "OrdStatus", "0",
                "CumQty", "0"
            )
            println(OrderNewRule.orders.keys.toString() + " amended " + ordID + ". Total:" + Companion.orders.size)
            ruleContext.send(fixAmend.build())
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