package com.exactpro.th2.sim.template.rule.test.api


import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.message.messageType
import com.google.protobuf.TextFormat
import org.junit.jupiter.api.Assertions
import org.junit.platform.commons.util.StringUtils
import org.opentest4j.AssertionFailedError


fun assertEqualsBatches(expected: MessageBatch, actual: MessageBatch, lazyMessage: () -> String? = {null}) {
    Assertions.assertEquals(expected.messagesCount, actual.messagesCount) {"wrong count of messages in batch: \n${TextFormat.shortDebugString(actual)}"}
    expected.messagesList.forEachIndexed { i, message ->
        try {
            assertEqualsMessages(message, actual.messagesList[i], lazyMessage)
        } catch (e: AssertionFailedError) {
            throw AssertionFailedError(
                "Error in message from batch with index '$i'.\n${e.message}",
                e.expected,
                e.actual,
                e.cause
            )
        }

    }
}

fun assertEqualsMessages(expected: Message, actual: Message, lazyMessage: () -> String? = {null}) {
    val assertExpected = expected.toBuilder().apply {
        metadataBuilder.timestampBuilder.resetTimestamp()
    }.build()
    val assertActual = actual.toBuilder().apply {
        metadataBuilder.timestampBuilder.resetTimestamp()
    }.build()
    try {
        Assertions.assertEquals(assertExpected, assertActual, lazyMessage)
    } catch (e: AssertionFailedError) {
        throw AssertionFailedError(
            "Error in message with type '${actual.messageType}'.\n${e.message}",
            e.expected,
            e.actual,
            e.cause
        )
    }

}

fun buildPrefix(message: String?): String {
    return if (StringUtils.isNotBlank(message)) "$message ==> " else ""
}

fun com.google.protobuf.Timestamp.Builder.resetTimestamp() {
    nanos = 0
    seconds = 0
}

