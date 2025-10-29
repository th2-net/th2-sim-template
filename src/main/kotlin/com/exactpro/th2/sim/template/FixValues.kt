/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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

class FixValues {
    companion object {
        const val SIDE_BUY = "1"
        const val SIDE_SELL = "2"
        const val EXEC_TYPE_NEW = "0"
        const val EXEC_TYPE_EXPIRED = "C"
        const val EXEC_TYPE_TRADE = "F"
        const val ORD_STATUS_NEW = "0"
        const val ORD_STATUS_PARTIALLY_FILLED = "1"
        const val ORD_STATUS_EXPIRED = "C"
        const val ORD_STATUS_FILLED = "2"
        const val TIME_IN_FORCE_DAY = "0"
        const val PARTY_ROLE_CLIENT_ID = "3"
        const val PARTY_ROLE_EXECUTING_TRADER = "12"
        const val PARTY_ROLE_CONTRA_FIRM = "17"
        const val PARTY_ROLE_DECK_ID = "76"
        const val PARTY_ROLE_INVESTMENT_DIVISION_MARKER = "122"
        const val PARTY_ID_SOURCE_PROPRIETARY_CUSTOM_CODE = "D"
        const val PARTY_ID_SOURCE_SHORT_CODE_IDENTIFIER = "P"
        const val SESSION_REJECT_REASON_REQUIRED_TAG_MISSING = "1"
        const val ORDER_CAPACITY_PRINCIPAL = "P"
        const val ACCOUNT_TYPE_NON_CUSTOMER = "2"
    }
}