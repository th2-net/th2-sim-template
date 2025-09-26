/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.sim.template.FixFields
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
                FixFields.ORDER_ID to orderId.incrementAndGet(),
                FixFields.EXEC_ID to execId.incrementAndGet(),
                FixFields.EXEC_TYPE to "2",
                FixFields.ORD_STATUS to "0",
                FixFields.CUM_QTY to "0",
                FixFields.TRADING_PARTY to null,
                FixFields.TRANSACT_TIME to LocalDateTime.now().toString(),
            ).copyFields(
                incomeMessage,
                FixFields.SIDE,
                FixFields.LEAVES_QTY,
                FixFields.CL_ORD_ID,
                FixFields.SECONDARY_CL_ORD_ID,
                FixFields.SECURITY_ID,
                FixFields.SECURITY_ID_SOURCE,
                FixFields.ORD_TYPE,
                FixFields.ORDER_QTY
            )
        )
    }
}