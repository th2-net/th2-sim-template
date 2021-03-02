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
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.copyFields
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.value.getInt
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.AbstractRule

class TemplateAbstractRule : AbstractRule() {

    override fun checkTriggered(input: Message): Boolean {
        input.getField("field1")?.getInt()?.also { a ->
            input.getField("field2")?.getInt()?.also { b ->
                return a + b > 80
            }
        }

        return false
    }

    override fun handle(context: IRuleContext, incomingMessage: Message) {
        return context.send(message("ExecutionReport").copyFields(incomingMessage, "field1", "field3").addField("field4", "value").build())
    }

}