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
import com.exactpro.th2.common.grpc.Message.Builder
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.*
import com.exactpro.th2.common.value.getMessage
import com.exactpro.th2.common.value.getString
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule
import java.util.UUID

import java.util.concurrent.atomic.AtomicInteger

class KotlinFIXRuleSecurity(field: Map<String, Value>) : MessageCompareRule() {
    private val entries = 10
    private val CouponRates = listOf<String>("4.25","1.25","3.125","2.75","3.75","5.25")
    private val SecurityTypes = listOf<String>("EUCD","FUT","PZFJ","TBILL")
    private val Issuers = listOf<String>("Fuji Climber Inc","Fujinomia Bank","MinamiDaito Inaka","Nara Deer","Enoshima Lighthouse")
    private val CreditRatings = listOf<String>("1","11","11+","111+")
    private val Symbols = listOf<String>("SYM_4.25_02/11/32_FUN","BOL_5.125_02/01/22_FUN","TAXM_4.25_04/05/42_PA","SYM_9.725_02/07/52_FUN")
    private val SecurityIDSources = listOf<String>("4","8","A","D")
    private val CFICodes = listOf<String>("DTOR","DPOV","MPAKH","APANF")
    private val CouponPaymentDates = listOf<String>("20220715","20220512","20220225","20220313")
    private val TradingSessionIDs = listOf<String>("1","2","3","4")
    private val Currencies = listOf<String>("GBP","USD","GEL","EUR")
    private val SecurityIDs = listOf<String>("UGA278942NDA","FWQ148064HTB","GFC356372LGT","PES249732JTR")
    private val CountryOfIssues = listOf<String>("USA","JAP","GBR","GEO")

    companion object {
        private var counter = 0
        private var fragment = ""
    }

    init {
        init("SecurityListRequest", field)
    }

    override fun handle(context: IRuleContext, incomeMessage: Message) {
        if (counter>3){
            fragment = "Y"
            counter=0
        }
        else{
            fragment="N"
            counter++
        }

        val msg = message("SecurityList").addFields(
            "SecurityReqID", incomeMessage.getField("SecurityReqID"),
            "SecurityResponseID", UUID.randomUUID().toString(),
            "LastFragment", fragment,
            "TotNoRelatedSym", entries,
            "SecurityRequestResult", "0",
            "NoRelatedSym",generateNoRelatedSym(entries)
        )
        context.send(msg.build())
    }
    private fun generateNoRelatedSym(num: Int): List<Builder> {
        val noRelatedSym:MutableList<Message.Builder> = mutableListOf()
        for(i in 0..num){
            noRelatedSym.add(generateEntry())

        }

        return noRelatedSym;
    }

    private fun generateEntry(): Message.Builder =
        message().addFields(
            "CouponRate", CouponRates.random(),
            "SecurityType", SecurityTypes.random(),
            "Issuer", Issuers.random(),
            "CreditRating", CreditRatings.random(),
            "Symbol", Symbols.random(),
            "SecurityIDSource", SecurityIDSources.random(),
            "CFICode", CFICodes.random(),
            "CouponPaymentDate", CouponPaymentDates.random(),
            "TradingSessionID", TradingSessionIDs.random(),
            "Currency", Currencies.random(),
            "SecurityID", SecurityIDs.random(),
            "CountryOfIssue", CountryOfIssues.random()
        )
}
