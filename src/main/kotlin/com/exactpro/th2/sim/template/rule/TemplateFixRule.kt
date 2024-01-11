/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.utils.message.transport.copyFields
import com.exactpro.th2.common.utils.message.transport.message
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger

class TemplateFixRule(field: Map<String, Any?>) : MessageCompareRule() {

    private var orderId = AtomicInteger(0)
    private var execId = AtomicInteger(0)

    init {
        init("NewOrderSingle", field)
    }

    override fun handle(context: IRuleContext, incomeMessage: ParsedMessage) {
        context.send(
            message("ExecutionReport").addFields(
                "OrderID" to orderId.incrementAndGet(),
                "ExecID" to execId.incrementAndGet(),
                "ExecType" to "2",
                "OrdStatus" to "0",
                "CumQty" to "0",
                "TradingParty" to null,
                "TransactTime" to LocalDateTime.now().toString(),
            ).copyFields(
                incomeMessage,
                "Side",
                "LeavesQty",
                "ClOrdID",
                "SecondaryClOrdID",
                "SecurityID",
                "SecurityIDSource",
                "OrdType",
                "OrderQty"
            )
        )
    }
}