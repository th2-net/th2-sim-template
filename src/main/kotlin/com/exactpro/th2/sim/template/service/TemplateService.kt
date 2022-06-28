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
package com.exactpro.th2.sim.template.service

import com.exactpro.th2.sim.ISimulator
import com.exactpro.th2.sim.ISimulatorPart
import com.exactpro.th2.sim.grpc.RuleID
import com.exactpro.th2.sim.template.grpc.SimTemplateGrpc
import com.exactpro.th2.sim.template.grpc.TemplateFixRuleCreate
import com.exactpro.th2.sim.template.rule.NOSRule
import com.exactpro.th2.sim.template.rule.AmendRule
import com.exactpro.th2.sim.template.rule.CancelRule
import com.exactpro.th2.sim.template.rule.QuoteRule
import com.exactpro.th2.sim.template.rule.SecurityRule
import com.exactpro.th2.sim.template.rule.DemoScriptRule
import com.exactpro.th2.sim.util.ServiceUtils
import io.grpc.stub.StreamObserver

class TemplateService : SimTemplateGrpc.SimTemplateImplBase(), ISimulatorPart {

    private lateinit var simulator: ISimulator

    override fun init(simulator: ISimulator) {
        this.simulator = simulator
    }

    override fun createNOSRule(request: TemplateFixRuleCreate, responseObserver: StreamObserver<RuleID>?) =
        ServiceUtils.addRule(NOSRule(request.fieldsMap), request.connectionId.sessionAlias, simulator, responseObserver)

    override fun createAmendRule(request: TemplateFixRuleCreate, responseObserver: StreamObserver<RuleID>?) =
        ServiceUtils.addRule(AmendRule(request.fieldsMap), request.connectionId.sessionAlias, simulator, responseObserver)

    override fun createCancelRule(request: TemplateFixRuleCreate, responseObserver: StreamObserver<RuleID>?) =
        ServiceUtils.addRule(CancelRule(request.fieldsMap), request.connectionId.sessionAlias, simulator, responseObserver)

    override fun createQuoteRule(request: TemplateFixRuleCreate, responseObserver: StreamObserver<RuleID>?) =
        ServiceUtils.addRule(QuoteRule(request.fieldsMap), request.connectionId.sessionAlias, simulator, responseObserver)

    override fun createSecurityRule(request: TemplateFixRuleCreate, responseObserver: StreamObserver<RuleID>?) =
        ServiceUtils.addRule(SecurityRule(request.fieldsMap), request.connectionId.sessionAlias, simulator, responseObserver)

    override fun createDemoScriptRule(request: TemplateFixRuleCreate, responseObserver: StreamObserver<RuleID>?) =
        ServiceUtils.addRule(DemoScriptRule(request.fieldsMap), request.connectionId.sessionAlias, simulator, responseObserver)
}
