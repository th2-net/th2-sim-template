/*
 * Copyright 2024-2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.sim.template

class FixFields {
    companion object {
        const val ORDER_ID = "OrderID"
        const val SYMBOL = "Symbol"
        const val MSG_TYPE = "MsgType"
        const val REF_TAG_ID = "RefTagID"
        const val REF_MSG_TYPE = "RefMsgType"
        const val REF_SEQ_NUM = "RefSeqNum"
        const val TEXT = "Text"

        const val SESSION_REJECT_REASON = "SessionRejectReason"
        const val HEADER = "header"
        const val SIDE = "Side"
        const val PRICE = "Price"
        const val CUM_QTY = "CumQty"
        const val CL_ORD_ID = "ClOrdID"
        const val SECONDARY_CL_ORD_ID = "SecondaryClOrdID"
        const val SECURITY_ID = "SecurityID"
        const val SECURITY_ID_SOURCE = "SecurityIDSource"
        const val ORD_TYPE = "OrdType"
        const val ORDER_QTY = "OrderQty"
        const val TRADING_PARTY = "TradingParty"
        const val TIME_IN_FORCE = "TimeInForce"
        const val ORDER_CAPACITY = "OrderCapacity"
        const val ACCOUNT_TYPE = "AccountType"
        const val TRANSACT_TIME = "TransactTime"
        const val EXEC_TYPE = "ExecType"
        const val ORD_STATUS = "OrdStatus"
        const val LAST_PX = "LastPx"
        const val LEAVES_QTY = "LeavesQty"
        const val EXEC_ID = "ExecID"
        const val TRD_MATCH_ID = "TrdMatchID"
        const val BUSINESS_REJECT_REASON = "BusinessRejectReason"
        const val BUSINESS_REJECT_REF_ID = "BusinessRejectRefID"
        const val PARTY_ROLE = "PartyRole"
        const val PARTY_ID = "PartyID"
        const val PARTY_ID_SOURCE = "PartyIDSource"
        const val AGGRESSOR_INDICATOR = "AggressorIndicator"

        const val SECURITY_STATUS_REQ_ID = "SecurityStatusReqID"
        const val CURRENCY = "Currency"
        const val MARKET_ID = "MarketID"
        const val MARKET_SEGMENT_ID = "MarketSegmentID"
        const val TRADING_SESSION_ID = "TradingSessionID"
        const val TRADING_SESSION_SUB_ID = "TradingSessionSubID"
        const val UNSOLICITED_INDICATOR = "UnsolicitedIndicator"
        const val SECURITY_TRADING_STATUS = "SecurityTradingStatus"
        const val BUY_VOLUME = "BuyVolume"
        const val SELL_VOLUME = "SellVolume"
        const val HIGH_PX = "HighPx"
        const val LOW_PX = "LowPx"
        const val FIRST_PX = "FirstPx"
        const val NO_PARTY_IDS = "NoPartyIDs"


        const val BEGIN_STRING = "BeginString"
        const val MSG_SEQ_NUM = "MsgSeqNum"
    }
}