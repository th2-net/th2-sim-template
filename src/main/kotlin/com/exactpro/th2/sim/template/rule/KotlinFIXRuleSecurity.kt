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
import com.exactpro.th2.common.utils.message.transport.containsField
import com.exactpro.th2.common.utils.message.transport.getFieldSoft
import com.exactpro.th2.common.utils.message.transport.getString
import com.exactpro.th2.common.utils.message.transport.message
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule
import com.exactpro.th2.sim.template.FixFields

class KotlinFIXRuleSecurity(field: Map<String, Any?>) : MessageCompareRule() {

    init {
        init("SecurityStatusRequest", field)
    }

    override fun handle(context: IRuleContext, incomeMessage: ParsedMessage) {
        if (!incomeMessage.containsField(FixFields.SECURITY_ID)) {
            val reject = message("Reject").addFields(
                FixFields.REF_TAG_ID to "48",
                FixFields.REF_MSG_TYPE to "f",
                FixFields.REF_SEQ_NUM to incomeMessage.getFieldSoft(FixFields.BEGIN_STRING, FixFields.MSG_SEQ_NUM),
                FixFields.TEXT to "Incorrect instrument",
                FixFields.SESSION_REJECT_REASON to "99"
            )
            context.send(reject)
        } else {
            if (incomeMessage.getString(FixFields.SECURITY_ID) == "INSTR6") {
                val unknownInstr = message("SecurityStatus").addFields(
                    FixFields.SECURITY_ID to incomeMessage.getString(FixFields.SECURITY_ID)!!,
                    FixFields.SECURITY_ID_SOURCE to incomeMessage.getString(FixFields.SECURITY_ID_SOURCE)!!,
                    FixFields.SECURITY_STATUS_REQ_ID to incomeMessage.getString(FixFields.SECURITY_STATUS_REQ_ID)!!,
                    FixFields.UNSOLICITED_INDICATOR to "N",
                    FixFields.SECURITY_TRADING_STATUS to "20",
                    FixFields.TEXT to "Unknown or Invalid instrument"
                )
                context.send(unknownInstr)
            } else {
                val securityStatus1 = message("SecurityStatus").addFields(
                    FixFields.SECURITY_ID to incomeMessage.getString(FixFields.SECURITY_ID)!!,
                    FixFields.SECURITY_ID_SOURCE to incomeMessage.getString(FixFields.SECURITY_ID_SOURCE)!!,
                    FixFields.SECURITY_STATUS_REQ_ID to incomeMessage.getString(FixFields.SECURITY_STATUS_REQ_ID)!!,
                    FixFields.CURRENCY to "JPY",
                    FixFields.MARKET_ID to "Demo Market",
                    FixFields.MARKET_SEGMENT_ID to "NEW",
                    FixFields.TRADING_SESSION_ID to "1",
                    FixFields.TRADING_SESSION_SUB_ID to "3",
                    FixFields.UNSOLICITED_INDICATOR to "N",
                    FixFields.SECURITY_TRADING_STATUS to "17",
                    FixFields.BUY_VOLUME to "0",
                    FixFields.SELL_VOLUME to "0",
                    FixFields.HIGH_PX to "56",
                    FixFields.LOW_PX to "54",
                    FixFields.LAST_PX to "54",
                    FixFields.FIRST_PX to "54",
                    FixFields.TEXT to "The simulated SecurityStatus has been sent"
                )
                context.send(securityStatus1)
            }
        }
    }
}