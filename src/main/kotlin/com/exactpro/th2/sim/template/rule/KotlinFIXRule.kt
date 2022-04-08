/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/

package com.exactpro.th2.sim.template.rule

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.addFields
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule
import java.util.UUID

class KotlinFIXRule(field: Map<String, Value>) : MessageCompareRule() {

    companion object {
        var reject = false
        var secondAlias = "dc-demo-server1"
    }

    init {
        init("Quote", field)
    }

    override fun handle(context: IRuleContext, incomeMessage: Message) {
        if (reject) {
            context.send(
                message("QuoteStatusReport").addFields(
                    "QuoteID", incomeMessage.getField("QuoteID"),
                    "SecurityID", incomeMessage.getField("SecurityID"),
                    "SecurityIDSource", incomeMessage.getField("SecurityIDSource"),
                    "Symbol", incomeMessage.getField("Symbol"),
                    "QuoteID", incomeMessage.getField("QuoteID"),
                    "QuoteStatus", "5",
                    "Text", "You already have an indication on this side"
                ).build()
            )
            reject = false
        } else {
            context.send(
                message("QuoteStatusReport").addFields(
                    "QuoteID", incomeMessage.getField("QuoteID"),
                    "SecurityID", incomeMessage.getField("SecurityID"),
                    "SecurityIDSource", incomeMessage.getField("SecurityIDSource"),
                    "Symbol", incomeMessage.getField("Symbol"),
                    "QuoteID", incomeMessage.getField("QuoteID"),
                    "QuoteStatus", "0",
                ).build()
            )
            context.send(
                message("Quote").addFields(
                    "BidPx", incomeMessage.getField("BidPx"),
                    "BidSize", incomeMessage.getField("BidSize"),
                    "OfferPx", incomeMessage.getField("OfferPx"),
                    "OfferSize", incomeMessage.getField("OfferSize"),
                    "QuoteType", incomeMessage.getField("QuoteType"),
                    "SecurityID", incomeMessage.getField("SecurityID"),
                    "SecurityIDSource", incomeMessage.getField("SecurityIDSource"),
                    "Symbol", incomeMessage.getField("Symbol"),
                    "QuoteID", UUID.randomUUID().toString(),
                    "NoPartyIDs", createNoPartyIdsList(secondAlias),
                    "NoQuoteQualifiers", incomeMessage.getField("NoQuoteQualifiers")
                ).buildWith { sessionAlias = secondAlias }
            )
            reject = true
        }
        return
    }

    private fun createNoPartyIdsList(firm: String): List<Message.Builder> = listOf(
        createPartyMessage("17", firm, "D"),
    )

    private fun createPartyMessage(partyRole: String, partyID: String, partyIDSource: String): Message.Builder =
        message().addFields(
            "PartyRole", partyRole,
            "PartyID", partyID,
            "PartyIDSource", partyIDSource
        )

    private fun Message.Builder.buildWith(action: Message.Builder.() -> Unit): Message {
        action(this)
        return this.build()
    }
}
