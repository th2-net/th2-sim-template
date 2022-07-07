package com.exactpro.th2.sim.template.rule

import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.*
import com.exactpro.th2.common.value.getInt
import com.exactpro.th2.common.value.getMessage
import com.exactpro.th2.common.value.getString
import com.exactpro.th2.sim.rule.IRuleContext
import com.exactpro.th2.sim.rule.impl.MessageCompareRule
import org.slf4j.LoggerFactory
import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream
import java.lang.Exception
import java.time.LocalDateTime

import java.util.concurrent.atomic.AtomicInteger

class DemoScriptRule(field: Map<String, Value>) : MessageCompareRule() {

    private var root = File(System.getProperty("user.home")+File.separator+"demo_outputs")
    private var csvFile = File(root, "csv_test.csv")
    private var values: String? = null

    companion object {
        val LOGGER = LoggerFactory.getLogger(DemoScriptRule::class.java.name)
        private var orderId = AtomicInteger(0)
        private var execId = AtomicInteger(0)
        private var TrdMatchId = AtomicInteger(0)
        private var incomeMsgList = arrayListOf<Message>()
        private var ordIdList = arrayListOf<Int>()
        private var writable = false
    }

    init {
        init("NewOrderSingle", field)
    }

    private fun updateFile(record: String, newLine: Boolean = true) {
        if (csvFile.exists() and !writable){
            csvFile.delete()
            writable=true
        }
        var writer: BufferedWriter? = null
        var stream: FileOutputStream? = null
        root.mkdir()
        if (csvFile.createNewFile()) {
            updateFile("CsvRecordType,OrderQty,OrdType,SecurityIDSource,ClOrdID,OrderCapacity,AccountType,Side,Price,SecurityID,TransactTime,SecondaryClOrdID,OrderID,ExecID,LeavesQty,Text,ExecType,OrdStatus,CumQty", false)
        }
        try {
            stream = FileOutputStream(csvFile, true)
            writer = stream.bufferedWriter()
            if (newLine) {
                writer.newLine()
            }
            writer.write(record)
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            writer!!.flush()
            stream?.close()
        }
    }
    private fun logMessages(message: MutableMap<String, String?>){
        LOGGER.debug("Message received: " +
                "[8=FIXT.1.1\u0001" +
                "9="+message["BodyLength"] +"\u000135=D\u0001" +
                "34="+message["MsgSeqNum"] +"\u0001" +
                "49="+message["Sender"] +"\u0001" +
                "52="+message["SendingTime"] +"\u000156=FGW\u0001" +
                "11="+message["ClOrdID"] +"\u0001" +
                "22="+message["SecurityIDSource"] +"\u0001" +
                "38="+message["OrderQty"] +"\u0001" +
                "40="+ message["OrdType"] +"\u0001" +
                "44="+ message["Price"] +"\u0001" +
                "48="+ message["SecurityID"] +"\u0001" +
                "54="+ message["Side"] +"\u0001" +
                "59="+ message["TimeInForce"] +"\u0001" +
                "60="+ message["TransactTime"] +"\u0001" +
                "526="+ message["SecondaryClOrdID"] +"\u0001" +
                "528="+ message["OrderCapacity"] +"\u0001" +
                "581="+ message["AccountType"] +"\u0001" +
                "453=4\u0001" +
                "448="+ message["Sender"] +"\u0001447=D\u0001452=76\u0001448=0\u0001447=P\u0001452=3\u0001448=0\u0001447=P\u0001452=122\u0001448=3\u0001447=P\u0001452=12\u0001" +
                "10="+message["CheckSum"] +"\u0001]")
    }
    private fun timeFormatDirtyFix(str: String?): String {
        var string = str.toString()
        string = string.replace("-", "")
        string = string.replace("T", "-")
        if (!string.contains('.')){return "$string.000"
        }
        return string
    }
    private fun withdrawMessage(incomeMessage: Message): MutableMap<String, String?> {
        var toLog: MutableMap<String, String?> = mutableMapOf()
        for (field in listOf( "ClOrdID", "SecurityIDSource", "OrderQty", "OrdType", "Price" ,"SecurityID", "Side", "TimeInForce", "SecondaryClOrdID" , "OrderCapacity" , "AccountType")){
            toLog[field] = incomeMessage.getFieldsOrDefault(field, Value.newBuilder().setSimpleValue("").build())!!.getString()
        }
        for (field in listOf("BodyLength", "MsgSeqNum")){
            toLog[field] = incomeMessage.getField("header")!!.getMessage()?.getFieldsOrDefault(field, Value.newBuilder().setSimpleValue("").build())!!.getString()
        }
        toLog["SendingTime"] = timeFormatDirtyFix(incomeMessage.getField("header")!!.getMessage()?.getFieldsOrDefault("SendingTime", Value.newBuilder().setSimpleValue("").build())!!.getString())
        toLog["TransactTime"] = timeFormatDirtyFix(incomeMessage.getFieldsOrDefault("TransactTime", Value.newBuilder().setSimpleValue("").build())!!.getString())
        toLog["CheckSum"] = incomeMessage.getField("trailer")!!.getMessage()?.getField("CheckSum")!!.getString()
        toLog["Sender"] = "DEMO-CONN"+incomeMessage.metadata.id.connectionId.sessionAlias.last()
        return toLog
    }
    override fun handle(context: IRuleContext, incomeMessage: Message) {
        incomeMsgList.add(incomeMessage)
        while (incomeMsgList.size > 3) {
            incomeMsgList.removeAt(0)
        }
        val ordId1 = orderId.incrementAndGet()

        ordIdList.add(ordId1)
        while (ordIdList.size > 3) {
            ordIdList.removeAt(0)
        }

        if (!incomeMessage.containsFields("Side")) {  // Empty Side tag should be rejected.
            val rej = message("Reject").addFields(
                    "RefTagID", "453",
                    "RefMsgType", "D",
                    "RefSeqNum", incomeMessage.getField("BeginString")?.getMessage()?.getField("MsgSeqNum"),
                    "Text", "Simulating reject message",
                    "SessionRejectReason", "1"
            )
            //rej.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
            context.send(rej.build())
        } else {
            when (incomeMessage.getString("SecurityID")) {
                "INSTR4" -> {  // Extra FIX ER
                    when (incomeMessage.getString("Side")) {
                        "1" -> {
                            val execIdNew = execId.incrementAndGet()
                            val transTime = LocalDateTime.now().toString()
                            val fixNew = message("ExecutionReport")
                                    .copyFields(incomeMessage,  // fields from NewOrder
                                            "Side",
                                            "Price",
                                            "CumQty",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderQty",
                                            "TradingParty",
                                            "TimeInForce",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime,
                                            "OrderID", ordId1,
                                            "ExecID", execIdNew,
                                            "LeavesQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "Text", "Simulated New Order Buy is placed",
                                            "ExecType", "0",
                                            "OrdStatus", "0",
                                            "CumQty", "0"
                                    )
                            //fixNew.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(fixNew.build())
                            logMessages(withdrawMessage(incomeMessage))

                            values = incomeMessage.getField("OrderQty")!!.getString() + "," +
                                    incomeMessage.getField("OrdType")!!.getString() + "," +
                                    incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                                    incomeMessage.getField("ClOrdID")!!.getString() + "," +
                                    incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                                    incomeMessage.getField("AccountType")!!.getString() + "," +
                                    incomeMessage.getField("Side")!!.getString() + "," +
                                    incomeMessage.getField("Price")!!.getString() + "," +
                                    incomeMessage.getField("SecurityID")!!.getString()
                            updateFile("D" + "," + values + "," +
                                    incomeMessage.getField("TransactTime")!!.getString() + "," +
                                    incomeMessage.getField("SecondaryClOrdID")!!.getString() + ",,,,,,,")
                            updateFile("8,$values,$transTime,,$ordId1,$execIdNew," +
                                    incomeMessage.getField("OrderQty")!!.getString() +
                                    ",Simulated New Order Buy is placed,0,0,0")
                            // DropCopy
                            val dcNew = message("ExecutionReport", Direction.FIRST, "dc-demo-server1")
                                    .copyFields(incomeMessage,  // fields from NewOrder
                                            "Side",
                                            "Price",
                                            "CumQty",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderQty",
                                            "TradingParty",
                                            "TimeInForce",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime,
                                            "OrderID", ordId1,
                                            "ExecID", execId.incrementAndGet(),
                                            "LeavesQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "Text", "Simulated New Order Buy is placed",
                                            "ExecType", "0",
                                            "OrdStatus", "0",
                                            "CumQty", "0"
                                    )
                            dcNew.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(dcNew.build())
                        }
                        "2" -> {
                            // Useful variables for buy-side
                            val cumQty1 = incomeMsgList[1].getField("OrderQty")!!.getInt()!!
                            val cumQty2 = incomeMsgList[0].getField("OrderQty")!!.getInt()!!
                            val leavesQty1 = incomeMessage.getField("OrderQty")!!.getInt()!! - cumQty1
                            val leavesQty2 = incomeMessage.getField("OrderQty")!!.getInt()!! - (cumQty1 + cumQty2)
                            val order1ClOdrID = incomeMsgList[0].getField("ClOrdID")!!.getString()
                            val order1Price = incomeMsgList[0].getField("Price")!!.getString()
                            val order1Qty = incomeMsgList[0].getField("OrderQty")!!.getString()
                            val order2ClOdrID = incomeMsgList[1].getField("ClOrdID")!!.getString()
                            val order2Price = incomeMsgList[1].getField("Price")!!.getString()
                            val order2Qty = incomeMsgList[1].getField("OrderQty")!!.getString()
                            val repeating1 = message().addFields("NoPartyIDs", listOf(
                                    message().addFields(
                                            "PartyRole", "76",
                                            "PartyID", "DEMO-CONN1",
                                            "PartyIDSource", "D"
                                    ),
                                    message().addFields(
                                            "PartyRole", "17",
                                            "PartyID", "DEMOFIRM2",
                                            "PartyIDSource", "D"
                                    ),
                                    message().addFields(
                                            "PartyRole", "3",
                                            "PartyID", "0",
                                            "PartyIDSource", "P"
                                    ),
                                    message().addFields(
                                            "PartyRole", "122",
                                            "PartyID", "0",
                                            "PartyIDSource", "P"
                                    ),
                                    message().addFields(
                                            "PartyRole", "12",
                                            "PartyID", "3",
                                            "PartyIDSource", "P"
                                    )
                            )
                            )
                            val tradeMatchID1 = TrdMatchId.incrementAndGet()
                            val tradeMatchID2 = TrdMatchId.incrementAndGet()
                            // Generator ER
                            // ER FF Order2 for Trader1
                            val execReportId1 = execId.incrementAndGet()
                            val transTime1 = LocalDateTime.now().toString()
                            val trader1Order2fix1 = message("ExecutionReport", Direction.FIRST, "fix-demo-server1")
                                    .copyFields(incomeMessage,
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime1,
                                            "TradingParty", repeating1,
                                            "TimeInForce", "0",  // Get from message?
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "CumQty", cumQty1,
                                            "OrderQty", order2Qty,
                                            "Price", order2Price,
                                            "LastPx", order2Price,
                                            "Side", "1",
                                            "LeavesQty", "0",
                                            "ClOrdID", order2ClOdrID,
                                            "OrderID", ordIdList[1],
                                            "ExecID", execReportId1,
                                            "TrdMatchID", tradeMatchID1,
                                            "Text", "The simulated order has been fully traded"
                                    )
                            trader1Order2fix1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader1Order2fix1.build())
                            logMessages(withdrawMessage(incomeMessage))

                            values = incomeMessage.getField("OrderQty")!!.getString() + "," +
                                    incomeMessage.getField("OrdType")!!.getString() + "," +
                                    incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                                    incomeMessage.getField("ClOrdID")!!.getString() + "," +
                                    incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                                    incomeMessage.getField("AccountType")!!.getString() + "," +
                                    incomeMessage.getField("Side")!!.getString() + "," +
                                    incomeMessage.getField("Price")!!.getString() + "," +
                                    incomeMessage.getField("SecurityID")!!.getString()
                            updateFile("D" + "," + values + "," +
                                    incomeMessage.getField("TransactTime")!!.getString() + "," +
                                    incomeMessage.getField("SecondaryClOrdID")!!.getString() + ",,,,,,,")
                            updateFile("8," +
                                    "$order2Qty," +
                                    incomeMessage.getField("OrdType")!!.getString() + "," +
                                    incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                                    "$order2ClOdrID," +
                                    incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                                    incomeMessage.getField("AccountType")!!.getString() +
                                    ",1,$order2Price," +
                                    incomeMessage.getField("SecurityID")!!.getString() +
                                    ",$transTime1,," + ordIdList[1] + ",$execReportId1,0,The simulated order has been fully traded,F,2,$cumQty1")
                            //DropCopy
                            val trader1Order2dc1 = message("ExecutionReport", Direction.FIRST, "dc-demo-server1")
                                    .copyFields(incomeMessage,
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime1,
                                            "TradingParty", repeating1,
                                            "TimeInForce", "0",  // Get from message?
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "CumQty", cumQty1,
                                            "OrderQty", order2Qty,
                                            "Price", order2Price,
                                            "LastPx", order2Price,
                                            "Side", "1",
                                            "LeavesQty", "0",
                                            "ClOrdID", order2ClOdrID,
                                            "OrderID", ordIdList[1],
                                            "ExecID", execReportId1,
                                            "TrdMatchID", tradeMatchID1,
                                            "Text", "The simulated order has been fully traded"
                                    )
                            trader1Order2dc1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader1Order2dc1.build())
                            // ER FF Order1 for Trader1
                            val execReportId2 = execId.incrementAndGet()
                            val transTime2 = LocalDateTime.now().toString()
                            val trader1Order1fix1 = message("ExecutionReport", Direction.FIRST, "fix-demo-server1")
                                    .copyFields(incomeMessage,
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating1,
                                            "TimeInForce", "0",  // Get from message?
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "CumQty", cumQty2,
                                            "OrderQty", order1Qty,
                                            "Price", order1Price,
                                            "LastPx", order1Price,
                                            "Side", "1",
                                            "ClOrdID", order1ClOdrID,
                                            "LeavesQty", "0",
                                            "OrderID", ordIdList[0],
                                            "ExecID", execReportId2,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "The simulated order has been fully traded"
                                    )
                            trader1Order1fix1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader1Order1fix1.build())
                            //DropCopy
                            val trader1Order1dc1 = message("ExecutionReport", Direction.FIRST, "dc-demo-server1")
                                    .copyFields(incomeMessage,
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating1,
                                            "TimeInForce", "0",  // Get from message?
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "CumQty", cumQty2,
                                            "OrderQty", order1Qty,
                                            "Price", order1Price,
                                            "LastPx", order1Price,
                                            "Side", "1",
                                            "ClOrdID", order1ClOdrID,
                                            "LeavesQty", "0",
                                            "OrderID", ordIdList[0],
                                            "ExecID", execReportId2,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "The simulated order has been fully traded"
                                    )
                            trader1Order1dc1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader1Order1dc1.build())
                            // ER1 PF Order3 for Trader2
                            val repeating2 = message().addFields("NoPartyIDs", listOf(
                                    message().addFields(
                                            "PartyRole", "76",
                                            "PartyID", "DEMO-CONN2",
                                            "PartyIDSource", "D"
                                    ),
                                    message().addFields(
                                            "PartyRole", "17",
                                            "PartyID", "DEMOFIRM1",
                                            "PartyIDSource", "D"
                                    ),
                                    message().addFields(
                                            "PartyRole", "3",
                                            "PartyID", "0",
                                            "PartyIDSource", "P"
                                    ),
                                    message().addFields(
                                            "PartyRole", "122",
                                            "PartyID", "0",
                                            "PartyIDSource", "P"
                                    ),
                                    message().addFields(
                                            "PartyRole", "12",
                                            "PartyID", "3",
                                            "PartyIDSource", "P"
                                    )
                            )
                            )
                            val execReportId3 = execId.incrementAndGet()
                            val trader2Order3fix1 = message("ExecutionReport", Direction.FIRST, "fix-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime1,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "1",
                                            "LastPx", order2Price,
                                            "CumQty", cumQty1,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty1,
                                            "OrderID", ordId1,
                                            "ExecID", execReportId3,
                                            "TrdMatchID", tradeMatchID1,
                                            "Text", "The simulated order has been partially traded"
                                    )
                            trader2Order3fix1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3fix1.build())
                            //DropCopy
                            val trader2Order3dc1 = message("ExecutionReport", Direction.FIRST, "dc-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime1,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "1",
                                            "LastPx", order2Price,
                                            "CumQty", cumQty1,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty1,
                                            "OrderID", ordId1,
                                            "ExecID", execReportId3,
                                            "TrdMatchID", tradeMatchID1,
                                            "Text", "The simulated order has been partially traded"
                                    )
                            trader2Order3dc1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3dc1.build())
                            // ER2 PF Order3 for Trader2
                            val execReportId4 = execId.incrementAndGet()
                            val trader2Order3fix2 = message("ExecutionReport", Direction.FIRST, "fix-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "1",
                                            "LastPx", order1Price,
                                            "CumQty", cumQty1 + cumQty2,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty2,
                                            "OrderID", ordId1,
                                            "ExecID", execReportId4,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "The simulated order has been partially traded"
                                    )
                            trader2Order3fix2.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3fix2.build())
                            //DropCopy
                            val trader2Order3dc2 = message("ExecutionReport", Direction.FIRST, "dc-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "1",
                                            "LastPx", order1Price,
                                            "CumQty", cumQty1 + cumQty2,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty2,
                                            "OrderID", ordId1,
                                            "ExecID", execReportId4,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "The simulated order has been partially traded"
                                    )
                            trader2Order3dc2.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3dc2.build())
                            // Extra ER3 FF Order3 for Trader2 as testcase
                            val execReportIdX = execId.incrementAndGet()
                            val trader2Order3fixX = message("ExecutionReport", Direction.FIRST, "fix-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "LastPx", order1Price,
                                            "CumQty", cumQty1 + cumQty2,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty2,
                                            "OrderID", ordId1,
                                            "ExecID", execReportIdX,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "Extra Execution Report"
                                    )
                            trader2Order3fixX.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3fixX.build())
                            // ER3 CC Order3 for Trader2
                            val execReportId5 = execId.incrementAndGet()
                            val transTime3 = LocalDateTime.now().toString()
                            val trader2Order3fix3 = message("ExecutionReport", Direction.FIRST, "fix-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "TradingParty",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime3,
                                            "ExecType", "C",
                                            "OrdStatus", "C",
                                            "CumQty", cumQty1 + cumQty2,
                                            "LeavesQty", "0",
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "OrderID", ordId1,
                                            "ExecID", execReportId5,
                                            "Text", "The remaining part of simulated order has been expired"
                                    )
                            trader2Order3fix3.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3fix3.build())
                            //DropCopy
                            val trader2Order3dc3 = message("ExecutionReport", Direction.FIRST, "dc-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "TradingParty",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime3,
                                            "ExecType", "C",
                                            "OrdStatus", "C",
                                            "CumQty", cumQty1 + cumQty2,
                                            "LeavesQty", "0",
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "OrderID", ordId1,
                                            "ExecID", execReportId5,
                                            "Text", "The remaining part of simulated order has been expired"
                                    )
                            trader2Order3dc3.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3dc3.build())
                        }
                    }
                }
                "INSTR5" -> {  // Inconsistent value in FIX ER
                    when (incomeMessage.getString("Side")) {
                        "1" -> {
                            val execIdNew = execId.incrementAndGet()
                            val transTime = LocalDateTime.now().toString()
                            val fixNew = message("ExecutionReport")
                                    .copyFields(incomeMessage,  // fields from NewOrder
                                            "Side",
                                            "Price",
                                            "CumQty",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderQty",
                                            "TradingParty",
                                            "TimeInForce",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime,
                                            "OrderID", ordId1,
                                            "ExecID", execIdNew,
                                            "LeavesQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "Text", "Simulated New Order Buy is placed",
                                            "ExecType", "0",
                                            "OrdStatus", "0",
                                            "CumQty", "0"
                                    )
                            fixNew.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(fixNew.build())
                            logMessages(withdrawMessage(incomeMessage))

                            values = incomeMessage.getField("OrderQty")!!.getString() + "," +
                                    incomeMessage.getField("OrdType")!!.getString() + "," +
                                    incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                                    incomeMessage.getField("ClOrdID")!!.getString() + "," +
                                    incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                                    incomeMessage.getField("AccountType")!!.getString() + "," +
                                    incomeMessage.getField("Side")!!.getString() + "," +
                                    incomeMessage.getField("Price")!!.getString() + "," +
                                    incomeMessage.getField("SecurityID")!!.getString()
                            updateFile("D" + "," + values + "," +
                                    incomeMessage.getField("TransactTime")!!.getString() + "," +
                                    incomeMessage.getField("SecondaryClOrdID")!!.getString() + ",,,,,,,")
                            updateFile("8,$values,$transTime,,$ordId1,$execIdNew," +
                                    incomeMessage.getField("OrderQty")!!.getString() +
                                    ",Simulated New Order Buy is placed,0,0,0")
                            // DropCopy
                            val dcNew = message("ExecutionReport", Direction.FIRST, "dc-demo-server1")
                                    .copyFields(incomeMessage,  // fields from NewOrder
                                            "Side",
                                            "Price",
                                            "CumQty",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderQty",
                                            "TradingParty",
                                            "TimeInForce",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime,
                                            "OrderID", ordId1,
                                            "ExecID", execId.incrementAndGet(),
                                            "LeavesQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "Text", "Simulated New Order Buy is placed",
                                            "ExecType", "0",
                                            "OrdStatus", "0",
                                            "CumQty", "0"
                                    )
                            dcNew.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(dcNew.build())
                        }
                        "2" -> {
                            // Useful variables for buy-side
                            val cumQty1 = incomeMsgList[1].getField("OrderQty")!!.getInt()!!
                            val cumQty2 = incomeMsgList[0].getField("OrderQty")!!.getInt()!!
                            val leavesQty1 = incomeMessage.getField("OrderQty")!!.getInt()!! - cumQty1
                            val leavesQty2 = incomeMessage.getField("OrderQty")!!.getInt()!! - (cumQty1 + cumQty2)
                            val order1ClOdrID = incomeMsgList[0].getField("ClOrdID")!!.getString()
                            val order1Price = incomeMsgList[0].getField("Price")!!.getString()
                            val order1Qty = incomeMsgList[0].getField("OrderQty")!!.getString()
                            val order2ClOdrID = incomeMsgList[1].getField("ClOrdID")!!.getString()
                            val order2Price = incomeMsgList[1].getField("Price")!!.getString()
                            val order2Qty = incomeMsgList[1].getField("OrderQty")!!.getString()
                            val repeating1 = message().addFields("NoPartyIDs", listOf(
                                    message().addFields(
                                            "PartyRole", "76",
                                            "PartyID", "DEMO-CONN1",
                                            "PartyIDSource", "D"
                                    ),
                                    message().addFields(
                                            "PartyRole", "17",
                                            "PartyID", "DEMOFIRM2",
                                            "PartyIDSource", "D"
                                    ),
                                    message().addFields(
                                            "PartyRole", "3",
                                            "PartyID", "0",
                                            "PartyIDSource", "P"
                                    ),
                                    message().addFields(
                                            "PartyRole", "122",
                                            "PartyID", "0",
                                            "PartyIDSource", "P"
                                    ),
                                    message().addFields(
                                            "PartyRole", "12",
                                            "PartyID", "3",
                                            "PartyIDSource", "P"
                                    )
                            )
                            )
                            val tradeMatchID1 = TrdMatchId.incrementAndGet()
                            val tradeMatchID2 = TrdMatchId.incrementAndGet()
                            // Generator ER
                            // ER FF Order2 for Trader1
                            val execReportId1 = execId.incrementAndGet()
                            val transTime1 = LocalDateTime.now().toString()
                            val trader1Order2fix1 = message("ExecutionReport", Direction.FIRST, "fix-demo-server1")
                                    .copyFields(incomeMessage,
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime1,
                                            "TradingParty", repeating1,
                                            "TimeInForce", "0",  // Get from message?
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "CumQty", cumQty1,
                                            "OrderQty", order2Qty,
                                            "Price", order2Price,
                                            "LastPx", order2Price,
                                            "Side", "1",
                                            "LeavesQty", "0",
                                            "ClOrdID", order2ClOdrID,
                                            "OrderID", ordIdList[1],
                                            "ExecID", execReportId1,
                                            "TrdMatchID", tradeMatchID1,
                                            "Text", "The simulated order has been fully traded"
                                    )
                            trader1Order2fix1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader1Order2fix1.build())
                            logMessages(withdrawMessage(incomeMessage))

                            values = incomeMessage.getField("OrderQty")!!.getString() + "," +
                                    incomeMessage.getField("OrdType")!!.getString() + "," +
                                    incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                                    incomeMessage.getField("ClOrdID")!!.getString() + "," +
                                    incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                                    incomeMessage.getField("AccountType")!!.getString() + "," +
                                    incomeMessage.getField("Side")!!.getString() + "," +
                                    incomeMessage.getField("Price")!!.getString() + "," +
                                    incomeMessage.getField("SecurityID")!!.getString()
                            updateFile("D" + "," + values + "," +
                                    incomeMessage.getField("TransactTime")!!.getString() + "," +
                                    incomeMessage.getField("SecondaryClOrdID")!!.getString() + ",,,,,,,")
                            updateFile("8," +
                                    "$order2Qty," +
                                    incomeMessage.getField("OrdType")!!.getString() + "," +
                                    incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                                    "$order2ClOdrID," +
                                    incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                                    incomeMessage.getField("AccountType")!!.getString() +
                                    ",1,$order2Price," +
                                    incomeMessage.getField("SecurityID")!!.getString() +
                                    ",$transTime1,," + ordIdList[1] + ",$execReportId1,0,The simulated order has been fully traded,F,2,$cumQty1")
                            //DropCopy
                            val trader1Order2dc1 = message("ExecutionReport", Direction.FIRST, "dc-demo-server1")
                                    .copyFields(incomeMessage,
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime1,
                                            "TradingParty", repeating1,
                                            "TimeInForce", "0",  // Get from message?
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "CumQty", cumQty1,
                                            "OrderQty", order2Qty,
                                            "Price", order2Price,
                                            "LastPx", order2Price,
                                            "Side", "1",
                                            "LeavesQty", "0",
                                            "ClOrdID", order2ClOdrID,
                                            "OrderID", ordIdList[1],
                                            "ExecID", execReportId1,
                                            "TrdMatchID", tradeMatchID1,
                                            "Text", "The simulated order has been fully traded"
                                    )
                            trader1Order2dc1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader1Order2dc1.build())
                            // ER FF Order1 for Trader1
                            val execReportId2 = execId.incrementAndGet()
                            val transTime2 = LocalDateTime.now().toString()
                            val trader1Order1fix1 = message("ExecutionReport", Direction.FIRST, "fix-demo-server1")
                                    .copyFields(incomeMessage,
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating1,
                                            "TimeInForce", "0",  // Get from message?
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "CumQty", cumQty2,
                                            "OrderQty", order1Qty,
                                            "Price", order1Price,
                                            "LastPx", order1Price,
                                            "Side", "1",
                                            "ClOrdID", order1ClOdrID,
                                            "LeavesQty", "0",
                                            "OrderID", ordIdList[0],
                                            "ExecID", execReportId2,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "The simulated order has been fully traded"
                                    )
                            trader1Order1fix1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader1Order1fix1.build())
                            //DropCopy
                            val trader1Order1dc1 = message("ExecutionReport", Direction.FIRST, "dc-demo-server1")
                                    .copyFields(incomeMessage,
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating1,
                                            "TimeInForce", "0",  // Get from message?
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "CumQty", cumQty2,
                                            "OrderQty", order1Qty,
                                            "Price", order1Price,
                                            "LastPx", order1Price,
                                            "Side", "1",
                                            "ClOrdID", order1ClOdrID,
                                            "LeavesQty", "0",
                                            "OrderID", ordIdList[0],
                                            "ExecID", execReportId2,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "The simulated order has been fully traded"
                                    )
                            trader1Order1dc1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader1Order1dc1.build())
                            // ER1 PF Order3 for Trader2
                            val repeating2 = message().addFields("NoPartyIDs", listOf(
                                    message().addFields(
                                            "PartyRole", "76",
                                            "PartyID", "DEMO-CONN2",
                                            "PartyIDSource", "D"
                                    ),
                                    message().addFields(
                                            "PartyRole", "17",
                                            "PartyID", "DEMOFIRM1",
                                            "PartyIDSource", "D"
                                    ),
                                    message().addFields(
                                            "PartyRole", "3",
                                            "PartyID", "0",
                                            "PartyIDSource", "P"
                                    ),
                                    message().addFields(
                                            "PartyRole", "122",
                                            "PartyID", "0",
                                            "PartyIDSource", "P"
                                    ),
                                    message().addFields(
                                            "PartyRole", "12",
                                            "PartyID", "3",
                                            "PartyIDSource", "P"
                                    )
                            )
                            )
                            val execReportId3 = execId.incrementAndGet()
                            val trader2Order3fix1 = message("ExecutionReport", Direction.FIRST, "fix-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime1,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "1",
                                            "LastPx", order2Price,
                                            "CumQty", cumQty1,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty1,
                                            "OrderID", ordId1,
                                            "ExecID", execReportId3,
                                            "TrdMatchID", tradeMatchID1,
                                            "Text", "The simulated order has been partially traded"
                                    )
                            trader2Order3fix1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3fix1.build())
                            //DropCopy
                            val trader2Order3dc1 = message("ExecutionReport", Direction.FIRST, "dc-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime1,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "1",
                                            "LastPx", order2Price,
                                            "CumQty", cumQty1,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty1,
                                            "OrderID", ordId1,
                                            "ExecID", execReportId3,
                                            "TrdMatchID", tradeMatchID1,
                                            "Text", "The simulated order has been partially traded"
                                    )
                            trader2Order3dc1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3dc1.build())
                            // ER2 PF Order3 for Trader2
                            val execReportId4 = execId.incrementAndGet()
                            val trader2Order3fix2 = message("ExecutionReport", Direction.FIRST, "fix-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "1",
                                            "LastPx", order1Price,
                                            "CumQty", cumQty1 + cumQty2,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty2,
                                            "OrderID", ordId1,
                                            "ExecID", execReportId4,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "Execution Report with incorrect value in OrdStatus tag",
                                            "OrderCapacity", "P",  // Incorrect value as testcase
                                            "AccountType", "2"     // Incorrect value as testcase
                                    )
                            trader2Order3fix2.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3fix2.build())
                            //DropCopy
                            val trader2Order3dc2 = message("ExecutionReport", Direction.FIRST, "dc-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "1",
                                            "LastPx", order1Price,
                                            "CumQty", cumQty1 + cumQty2,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty2,
                                            "OrderID", ordId1,
                                            "ExecID", execReportId4,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "The simulated order has been partially traded"
                                    )
                            trader2Order3dc2.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3dc2.build())
                            // ER3 CC Order3 for Trader2
                            val execReportId5 = execId.incrementAndGet()
                            val transTime3 = LocalDateTime.now().toString()
                            val trader2Order3fix3 = message("ExecutionReport", Direction.FIRST, "fix-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "TradingParty",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime3,
                                            "ExecType", "C",
                                            "OrdStatus", "C",
                                            "CumQty", cumQty1 + cumQty2,
                                            "LeavesQty", "0",
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "OrderID", ordId1,
                                            "ExecID", execReportId5,
                                            "Text", "The remaining part of simulated order has been expired"
                                    )
                            trader2Order3fix3.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3fix3.build())
                            //DropCopy
                            val trader2Order3dc3 = message("ExecutionReport", Direction.FIRST, "dc-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "TradingParty",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime3,
                                            "ExecType", "C",
                                            "OrdStatus", "C",
                                            "CumQty", cumQty1 + cumQty2,
                                            "LeavesQty", "0",
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "OrderID", ordId1,
                                            "ExecID", execReportId5,
                                            "Text", "The remaining part of simulated order has been expired"
                                    )
                            trader2Order3dc3.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3dc3.build())
                        }
                    }
                }
                "INSTR6" -> {  // Unexpected instrument
                    val bmrej = message("BusinessMessageReject").addFields(
                            "RefTagID", "48",
                            "RefMsgType", "D",
                            "RefSeqNum", incomeMessage.getField("BeginString")?.getMessage()?.getField("MsgSeqNum"),
                            "Text", "Unknown SecurityID",
                            "BusinessRejectReason", "2",
                            "BusinessRejectRefID", incomeMessage.getField("ClOrdID")!!.getString()
                    )
                    bmrej.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                    context.send(bmrej.build())
                    logMessages(withdrawMessage(incomeMessage))

                    values = incomeMessage.getField("OrderQty")!!.getString() + "," +
                            incomeMessage.getField("OrdType")!!.getString() + "," +
                            incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                            incomeMessage.getField("ClOrdID")!!.getString() + "," +
                            incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                            incomeMessage.getField("AccountType")!!.getString() + "," +
                            incomeMessage.getField("Side")!!.getString() + "," +
                            incomeMessage.getField("Price")!!.getString() + "," +
                            incomeMessage.getField("SecurityID")!!.getString()
                    updateFile("D" + "," + values + "," +
                            incomeMessage.getField("TransactTime")!!.getString() + "," +
                            incomeMessage.getField("SecondaryClOrdID")!!.getString() + ",,,,,,,")

                }
                "INSTR1", "INSTR2", "INSTR3" -> {  // Expectedly correct ERs
                    when (incomeMessage.getString("Side")) {
                        "1" -> {
                            val execIdNew = execId.incrementAndGet()
                            val transTime = LocalDateTime.now().toString()
                            val fixNew = message("ExecutionReport")
                                    .copyFields(incomeMessage,  // fields from NewOrder
                                            "Side",
                                            "Price",
                                            "CumQty",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderQty",
                                            "TradingParty",
                                            "TimeInForce",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime,
                                            "OrderID", ordId1,
                                            "ExecID", execIdNew,
                                            "LeavesQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "Text", "Simulated New Order Buy is placed",
                                            "ExecType", "0",
                                            "OrdStatus", "0",
                                            "CumQty", "0"
                                    )
                            fixNew.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(fixNew.build())
                            logMessages(withdrawMessage(incomeMessage))

                            values = incomeMessage.getField("OrderQty")!!.getString() + "," +
                                    incomeMessage.getField("OrdType")!!.getString() + "," +
                                    incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                                    incomeMessage.getField("ClOrdID")!!.getString() + "," +
                                    incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                                    incomeMessage.getField("AccountType")!!.getString() + "," +
                                    incomeMessage.getField("Side")!!.getString() + "," +
                                    incomeMessage.getField("Price")!!.getString() + "," +
                                    incomeMessage.getField("SecurityID")!!.getString()
                            updateFile("D" + "," + values + "," +
                                    incomeMessage.getField("TransactTime")!!.getString() + "," +
                                    incomeMessage.getField("SecondaryClOrdID")!!.getString() + ",,,,,,,")
                            updateFile("8,$values,$transTime,,$ordId1,$execIdNew," +
                                    incomeMessage.getField("OrderQty")!!.getString() +
                                    ",Simulated New Order Buy is placed,0,0,0")
                            // DropCopy
                            val dcNew = message("ExecutionReport", Direction.FIRST, "dc-demo-server1")
                                    .copyFields(incomeMessage,  // fields from NewOrder
                                            "Side",
                                            "Price",
                                            "CumQty",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderQty",
                                            "TradingParty",
                                            "TimeInForce",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime,
                                            "OrderID", ordId1,
                                            "ExecID", execId.incrementAndGet(),
                                            "LeavesQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "Text", "Simulated New Order Buy is placed",
                                            "ExecType", "0",
                                            "OrdStatus", "0",
                                            "CumQty", "0"
                                    )
                            dcNew.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(dcNew.build())
                        }
                        "2" -> {
                            // Useful variables for buy-side
                            val cumQty1 = incomeMsgList[1].getField("OrderQty")!!.getInt()!!
                            val cumQty2 = incomeMsgList[0].getField("OrderQty")!!.getInt()!!
                            val leavesQty1 = incomeMessage.getField("OrderQty")!!.getInt()!! - cumQty1
                            val leavesQty2 = incomeMessage.getField("OrderQty")!!.getInt()!! - (cumQty1 + cumQty2)
                            val order1ClOdrID = incomeMsgList[0].getField("ClOrdID")!!.getString()
                            val order1Price = incomeMsgList[0].getField("Price")!!.getString()
                            val order1Qty = incomeMsgList[0].getField("OrderQty")!!.getString()
                            val order2ClOdrID = incomeMsgList[1].getField("ClOrdID")!!.getString()
                            val order2Price = incomeMsgList[1].getField("Price")!!.getString()
                            val order2Qty = incomeMsgList[1].getField("OrderQty")!!.getString()
                            val repeating1 = message().addFields("NoPartyIDs", listOf(
                                    message().addFields(
                                            "PartyRole", "76",
                                            "PartyID", "DEMO-CONN1",
                                            "PartyIDSource", "D"
                                    ),
                                    message().addFields(
                                            "PartyRole", "17",
                                            "PartyID", "DEMOFIRM2",
                                            "PartyIDSource", "D"
                                    ),
                                    message().addFields(
                                            "PartyRole", "3",
                                            "PartyID", "0",
                                            "PartyIDSource", "P"
                                    ),
                                    message().addFields(
                                            "PartyRole", "122",
                                            "PartyID", "0",
                                            "PartyIDSource", "P"
                                    ),
                                    message().addFields(
                                            "PartyRole", "12",
                                            "PartyID", "3",
                                            "PartyIDSource", "P"
                                    )
                            )
                            )
                            val tradeMatchID1 = TrdMatchId.incrementAndGet()
                            val tradeMatchID2 = TrdMatchId.incrementAndGet()
                            // Generator ER
                            // ER FF Order2 for Trader1
                            val execReportId1 = execId.incrementAndGet()
                            val transTime1 = LocalDateTime.now().toString()
                            val trader1Order2fix1 = message("ExecutionReport", Direction.FIRST, "fix-demo-server1")
                                    .copyFields(incomeMessage,
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime1,
                                            "TradingParty", repeating1,
                                            "TimeInForce", "0",  // Get from message?
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "CumQty", cumQty1,
                                            "OrderQty", order2Qty,
                                            "Price", order2Price,
                                            "LastPx", order2Price,
                                            "Side", "1",
                                            "LeavesQty", "0",
                                            "ClOrdID", order2ClOdrID,
                                            "OrderID", ordIdList[1],
                                            "ExecID", execReportId1,
                                            "TrdMatchID", tradeMatchID1,
                                            "Text", "The simulated order has been fully traded"
                                    )
                            trader1Order2fix1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader1Order2fix1.build())
                            logMessages(withdrawMessage(incomeMessage))

                            values = incomeMessage.getField("OrderQty")!!.getString() + "," +
                                    incomeMessage.getField("OrdType")!!.getString() + "," +
                                    incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                                    incomeMessage.getField("ClOrdID")!!.getString() + "," +
                                    incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                                    incomeMessage.getField("AccountType")!!.getString() + "," +
                                    incomeMessage.getField("Side")!!.getString() + "," +
                                    incomeMessage.getField("Price")!!.getString() + "," +
                                    incomeMessage.getField("SecurityID")!!.getString()
                            updateFile("D" + "," + values + "," +
                                    incomeMessage.getField("TransactTime")!!.getString() + "," +
                                    incomeMessage.getField("SecondaryClOrdID")!!.getString() + ",,,,,,,")
                            updateFile("8," +
                                    "$order2Qty," +
                                    incomeMessage.getField("OrdType")!!.getString() + "," +
                                    incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                                    "$order2ClOdrID," +
                                    incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                                    incomeMessage.getField("AccountType")!!.getString() +
                                    ",1,$order2Price," +
                                    incomeMessage.getField("SecurityID")!!.getString() +
                                    ",$transTime1,," + ordIdList[1] + ",$execReportId1,0,The simulated order has been fully traded,F,2,$cumQty1")
                            //DropCopy
                            val trader1Order2dc1 = message("ExecutionReport", Direction.FIRST, "dc-demo-server1")
                                    .copyFields(incomeMessage,
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime1,
                                            "TradingParty", repeating1,
                                            "TimeInForce", "0",  // Get from message?
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "CumQty", cumQty1,
                                            "OrderQty", order2Qty,
                                            "Price", order2Price,
                                            "LastPx", order2Price,
                                            "Side", "1",
                                            "LeavesQty", "0",
                                            "ClOrdID", order2ClOdrID,
                                            "OrderID", ordIdList[1],
                                            "ExecID", execReportId1,
                                            "TrdMatchID", tradeMatchID1,
                                            "Text", "The simulated order has been fully traded"
                                    )
                            trader1Order2dc1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader1Order2dc1.build())
                            // ER FF Order1 for Trader1
                            val execReportId2 = execId.incrementAndGet()
                            val transTime2 = LocalDateTime.now().toString()
                            val trader1Order1fix1 = message("ExecutionReport", Direction.FIRST, "fix-demo-server1")
                                    .copyFields(incomeMessage,
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating1,
                                            "TimeInForce", "0",  // Get from message?
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "CumQty", cumQty2,
                                            "OrderQty", order1Qty,
                                            "Price", order1Price,
                                            "LastPx", order1Price,
                                            "Side", "1",
                                            "ClOrdID", order1ClOdrID,
                                            "LeavesQty", "0",
                                            "OrderID", ordIdList[0],
                                            "ExecID", execReportId2,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "The simulated order has been fully traded"
                                    )
                            trader1Order1fix1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader1Order1fix1.build())
                            updateFile("8," +
                                    "$order1Qty," +
                                    incomeMessage.getField("OrdType")!!.getString() + "," +
                                    incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                                    "$order1ClOdrID," +
                                    incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                                    incomeMessage.getField("AccountType")!!.getString() +
                                    ",1,$order1Price," +
                                    incomeMessage.getField("SecurityID")!!.getString() +
                                    ",$transTime2,," + ordIdList[0] + ",$execReportId2,0,The simulated order has been fully traded,F,2,$cumQty2")
                            //DropCopy
                            val trader1Order1dc1 = message("ExecutionReport", Direction.FIRST, "dc-demo-server1")
                                    .copyFields(incomeMessage,
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating1,
                                            "TimeInForce", "0",  // Get from message?
                                            "ExecType", "F",
                                            "OrdStatus", "2",
                                            "CumQty", cumQty2,
                                            "OrderQty", order1Qty,
                                            "Price", order1Price,
                                            "LastPx", order1Price,
                                            "Side", "1",
                                            "ClOrdID", order1ClOdrID,
                                            "LeavesQty", "0",
                                            "OrderID", ordIdList[0],
                                            "ExecID", execReportId2,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "The simulated order has been fully traded"
                                    )
                            trader1Order1dc1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader1Order1dc1.build())
                            // ER1 PF Order3 for Trader2
                            val repeating2 = message().addFields("NoPartyIDs", listOf(
                                    message().addFields(
                                            "PartyRole", "76",
                                            "PartyID", "DEMO-CONN2",
                                            "PartyIDSource", "D"
                                    ),
                                    message().addFields(
                                            "PartyRole", "17",
                                            "PartyID", "DEMOFIRM1",
                                            "PartyIDSource", "D"
                                    ),
                                    message().addFields(
                                            "PartyRole", "3",
                                            "PartyID", "0",
                                            "PartyIDSource", "P"
                                    ),
                                    message().addFields(
                                            "PartyRole", "122",
                                            "PartyID", "0",
                                            "PartyIDSource", "P"
                                    ),
                                    message().addFields(
                                            "PartyRole", "12",
                                            "PartyID", "3",
                                            "PartyIDSource", "P"
                                    )
                            )
                            )
                            val execReportId3 = execId.incrementAndGet()
                            val trader2Order3fix1 = message("ExecutionReport", Direction.FIRST, "fix-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime1,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "1",
                                            "LastPx", order2Price,
                                            "CumQty", cumQty1,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty1,
                                            "OrderID", ordId1,
                                            "ExecID", execReportId3,
                                            "TrdMatchID", tradeMatchID1,
                                            "Text", "The simulated order has been partially traded"
                                    )
                            trader2Order3fix1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3fix1.build())
                            updateFile("8," +
                                    incomeMessage.getField("OrderQty")!!.getString() + "," +
                                    incomeMessage.getField("OrdType")!!.getString() + "," +
                                    incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                                    incomeMessage.getField("ClOrdID")!!.getString() + "," +
                                    incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                                    incomeMessage.getField("AccountType")!!.getString() + "," +
                                    incomeMessage.getField("Side")!!.getString() + "," +
                                    incomeMessage.getField("Price")!!.getString() +
                                    "," +
                                    incomeMessage.getField("SecurityID")!!.getString() +
                                    ",$transTime1,," + ordId1 + ",$execReportId3,$leavesQty1,The simulated order has been partially traded,F,1,$cumQty1")
                            //DropCopy
                            val trader2Order3dc1 = message("ExecutionReport", Direction.FIRST, "dc-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime1,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "1",
                                            "LastPx", order2Price,
                                            "CumQty", cumQty1,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty1,
                                            "OrderID", ordId1,
                                            "ExecID", execReportId3,
                                            "TrdMatchID", tradeMatchID1,
                                            "Text", "The simulated order has been partially traded"
                                    )
                            trader2Order3dc1.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3dc1.build())
                            // ER2 PF Order3 for Trader2
                            val execReportId4 = execId.incrementAndGet()
                            val trader2Order3fix2 = message("ExecutionReport", Direction.FIRST, "fix-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "1",
                                            "LastPx", order1Price,
                                            "CumQty", cumQty1 + cumQty2,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty2,
                                            "OrderID", ordId1,
                                            "ExecID", execReportId4,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "The simulated order has been partially traded"
                                    )
                            trader2Order3fix2.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3fix2.build())
                            updateFile("8," +
                                    incomeMessage.getField("OrderQty")!!.getString() + "," +
                                    incomeMessage.getField("OrdType")!!.getString() + "," +
                                    incomeMessage.getField("SecurityIDSource")!!.getString() + "," +
                                    incomeMessage.getField("ClOrdID")!!.getString() + "," +
                                    incomeMessage.getField("OrderCapacity")!!.getString() + "," +
                                    incomeMessage.getField("AccountType")!!.getString() + "," +
                                    incomeMessage.getField("Side")!!.getString() + "," +
                                    incomeMessage.getField("Price")!!.getString() +
                                    "," +
                                    incomeMessage.getField("SecurityID")!!.getString() +
                                    ",$transTime2,," + ordId1 + ",$execReportId4,$leavesQty2,The simulated order has been partially traded,F,1," +
                                    (cumQty1 + cumQty2).toString())
                            //DropCopy
                            val trader2Order3dc2 = message("ExecutionReport", Direction.FIRST, "dc-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime2,
                                            "TradingParty", repeating2,
                                            "ExecType", "F",
                                            "OrdStatus", "1",
                                            "LastPx", order1Price,
                                            "CumQty", cumQty1 + cumQty2,
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "LeavesQty", leavesQty2,
                                            "OrderID", ordId1,
                                            "ExecID", execReportId4,
                                            "TrdMatchID", tradeMatchID2,
                                            "Text", "The simulated order has been partially traded"
                                    )
                            trader2Order3dc2.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3dc2.build())
                            // ER3 CC Order3 for Trader2
                            val execReportId5 = execId.incrementAndGet()
                            val transTime3 = LocalDateTime.now().toString()
                            val trader2Order3fix3 = message("ExecutionReport", Direction.FIRST, "fix-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "TradingParty",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime3,
                                            "ExecType", "C",
                                            "OrdStatus", "C",
                                            "CumQty", cumQty1 + cumQty2,
                                            "LeavesQty", "0",
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "OrderID", ordId1,
                                            "ExecID", execReportId5,
                                            "Text", "The remaining part of simulated order has been expired"
                                    )
                            trader2Order3fix3.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3fix3.build())
                            //DropCopy
                            val trader2Order3dc3 = message("ExecutionReport", Direction.FIRST, "dc-demo-server2")
                                    .copyFields(incomeMessage,
                                            "TimeInForce",
                                            "Side",
                                            "Price",
                                            "ClOrdID",
                                            "SecurityID",
                                            "SecurityIDSource",
                                            "OrdType",
                                            "TradingParty",
                                            "OrderCapacity",
                                            "AccountType"
                                    )
                                    .addFields(
                                            "TransactTime", transTime3,
                                            "ExecType", "C",
                                            "OrdStatus", "C",
                                            "CumQty", cumQty1 + cumQty2,
                                            "LeavesQty", "0",
                                            "OrderQty", incomeMessage.getField("OrderQty")!!.getString(),
                                            "OrderID", ordId1,
                                            "ExecID", execReportId5,
                                            "Text", "The remaining part of simulated order has been expired"
                                    )
                            trader2Order3dc3.parentEventId = EventID.newBuilder().setId(context.rootEventId).build()
                            context.send(trader2Order3dc3.build())
                        }
                    }
                }
            }
        }
    }
}
