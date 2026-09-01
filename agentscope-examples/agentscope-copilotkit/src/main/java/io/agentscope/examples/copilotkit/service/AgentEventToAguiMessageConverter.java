/*
 * Copyright 2024-2026 the original author or authors.
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
package io.agentscope.examples.copilotkit.service;

import com.fasterxml.jackson.core.type.TypeReference;
import io.agentscope.core.agui.converter.AguiMessageConverter;
import io.agentscope.core.agui.model.AguiFunctionCall;
import io.agentscope.core.agui.model.AguiMessage;
import io.agentscope.core.agui.model.AguiToolCall;
import io.agentscope.core.agui.model.MessageContent;
import io.agentscope.core.event.AgentEndEvent;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.AgentResultEvent;
import io.agentscope.core.event.ConfirmResult;
import io.agentscope.core.event.CustomEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import io.agentscope.core.event.ThinkingBlockDeltaEvent;
import io.agentscope.core.event.ToolCallDeltaEvent;
import io.agentscope.core.event.ToolCallEndEvent;
import io.agentscope.core.event.ToolCallStartEvent;
import io.agentscope.core.event.ToolResultDataDeltaEvent;
import io.agentscope.core.event.ToolResultEndEvent;
import io.agentscope.core.event.ToolResultStartEvent;
import io.agentscope.core.event.ToolResultTextDeltaEvent;
import io.agentscope.core.event.UserConfirmResultEvent;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultState;
import io.agentscope.core.util.JsonUtils;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Re-assembles persisted {@link AgentEvent}s into complete AG-UI {@link AguiMessage}s by folding
 * the streaming protocol the same way the live {@code AgentEvent → AguiEvent} adapter does.
 *
 * <p>The stream carries assistant output as incremental blocks ({@code THINKING_BLOCK_*} /
 * {@code TEXT_BLOCK_*} / {@code TOOL_CALL_*}) and tool execution as result deltas
 * ({@code TOOL_RESULT_*}). This converter accumulates those deltas and emits one final message
 * per logical unit, in event order:
 * <ul>
 *   <li>{@code CUSTOM(agui.user.input)} → user message;</li>
 *   <li>the thinking block of a reply → {@code reasoning} message ({@code replyId-reasoning});</li>
 *   <li>the text/tool-call blocks of a reply → {@code assistant} message with the full text and
 *       the tool calls issued by that reply;</li>
 *   <li>the result of a tool call → {@code tool} message ({@code replyId:toolCallId});</li>
 * </ul>
 * Lifecycle / model-call scaffolding events carry no visible content and are ignored; suspended
 * tool results (external execution pending) are skipped, matching the persistence middleware.
 */
@Component
public class AgentEventToAguiMessageConverter {

    /**
     * Custom event name used to persist each user input.
     */
    public static final String USER_INPUT_EVENT = "agui.user.input";

    /**
     * Reasoning message id suffix, matching {@code AguiStreamContext.REASONING_MESSAGE_ID_SUFFIX}.
     */
    private static final String REASONING_SUFFIX = "-reasoning";

    private final List<AguiMessage> messages = new ArrayList<>();
    private final Map<String, ToolResultBuffer> toolResults = new LinkedHashMap<>();

    // State of the reply (one model output round) currently being accumulated. Events of a new
    // reply flush the previous one first, so a single reply is active at any time.
    private String currentReplyId;
    private final StringBuilder currentReasoning = new StringBuilder();
    private final StringBuilder currentText = new StringBuilder();
    private final Map<String, ToolCallBuffer> currentToolCalls = new LinkedHashMap<>();
    private final AguiMessageConverter aguiMessageConverter = new AguiMessageConverter();

    /**
     * Converts a stored event sequence into complete AG-UI messages, preserving conversation
     * order.
     *
     * @param events the stored agent events of a thread (ordered by creation time)
     * @return the AG-UI messages for a history snapshot
     */
    public List<AguiMessage> convert(List<AgentEvent> events) {
        messages.clear();
        toolResults.clear();
        resetReply();
        for (AgentEvent event : events) {
            onEvent(event);
        }
        flushReply();
        flushToolResults();
        return List.copyOf(messages);
    }

    private void onEvent(AgentEvent event) {
        if (event instanceof CustomEvent customEvent
                && Objects.equals(customEvent.getName(), USER_INPUT_EVENT)) {
            flushReply();
            messages.addAll(aguiMessageConverter.toAguiMessageList(decodedInput(customEvent)));
            return;
        }
        if (event instanceof ThinkingBlockDeltaEvent delta) {
            switchReply(delta.getReplyId());
            currentReasoning.append(delta.getDelta());
            return;
        }
        if (event instanceof TextBlockDeltaEvent delta) {
            switchReply(delta.getReplyId());
            currentText.append(delta.getDelta());
            return;
        }
        if (event instanceof ToolCallStartEvent start) {
            switchReply(start.getReplyId());
            currentToolCalls
                    .computeIfAbsent(start.getToolCallId(), ignored -> new ToolCallBuffer())
                    .setName(start.getToolCallName());
            return;
        }
        if (event instanceof ToolCallDeltaEvent delta) {
            switchReply(delta.getReplyId());
            currentToolCalls
                    .computeIfAbsent(delta.getToolCallId(), ignored -> new ToolCallBuffer())
                    .appendArguments(delta.getDelta());
            return;
        }
        if (event instanceof ToolCallEndEvent end) {
            switchReply(end.getReplyId());
            ToolCallBuffer buffer =
                    currentToolCalls.computeIfAbsent(
                            end.getToolCallId(), ignored -> new ToolCallBuffer());
            buffer.setName(end.getToolCallName());
            buffer.complete();
            return;
        }
        if (event instanceof ToolResultStartEvent start) {
            // The assistant message of the calling reply (with its tool calls) must precede the
            // tool result in history.
            flushReply();
            toolResults.put(
                    start.getToolCallId(),
                    new ToolResultBuffer(start.getReplyId(), start.getToolCallId()));
            return;
        }
        if (event instanceof ToolResultTextDeltaEvent delta) {
            toolResults
                    .computeIfAbsent(
                            delta.getToolCallId(),
                            ignored ->
                                    new ToolResultBuffer(delta.getReplyId(), delta.getToolCallId()))
                    .appendText(delta.getDelta());
            return;
        }
        if (event instanceof ToolResultDataDeltaEvent delta) {
            toolResults
                    .computeIfAbsent(
                            delta.getToolCallId(),
                            ignored ->
                                    new ToolResultBuffer(delta.getReplyId(), delta.getToolCallId()))
                    .appendData(delta.getData());
            return;
        }
        if (event instanceof ToolResultEndEvent end) {
            ToolResultBuffer buffer = toolResults.get(end.getToolCallId());
            if (buffer == null) {
                return;
            }
            toolResults.remove(end.getToolCallId());
            // RUNNING marks a result suspended for external execution: no usable content, it is
            // not part of the visible history.
            if (end.getState() != ToolResultState.RUNNING) {
                messages.add(buffer.toToolMessage());
            }
            return;
        }
        if (event instanceof UserConfirmResultEvent userConfirmResultEvent) {
            for (ConfirmResult confirmResult : userConfirmResultEvent.getConfirmResults()) {
                toolResults
                        .computeIfAbsent(
                                confirmResult.getToolCall().getId(),
                                ignored ->
                                        new ToolResultBuffer(
                                                userConfirmResultEvent.getReplyId(),
                                                confirmResult.getToolCall().getId()))
                        .appendText(confirmResult.getToolCall().getContent());
            }
        }
        if (event instanceof AgentResultEvent || event instanceof AgentEndEvent) {
            flushReply();
            flushToolResults();
        }
    }

    /**
     * Flushes the current reply when a block of a different reply id arrives.
     */
    private void switchReply(String replyId) {
        if (!Objects.equals(replyId, currentReplyId)) {
            flushReply();
            currentReplyId = replyId;
        }
    }

    /**
     * Emits the complete messages of the reply being accumulated (reasoning first, assistant
     * second) and resets the reply state.
     */
    private void flushReply() {
        if (currentReplyId == null) {
            return;
        }
        String reasoning = currentReasoning.toString();
        String text = currentText.toString();
        List<AguiToolCall> toolCalls =
                currentToolCalls.entrySet().stream()
                        .filter(
                                entry ->
                                        entry.getValue().isComplete()
                                                || entry.getValue().hasArguments())
                        .map(entry -> entry.getValue().toAguiToolCall(entry.getKey()))
                        .toList();
        if (!reasoning.isEmpty()) {
            messages.add(
                    new AguiMessage(
                            currentReplyId + REASONING_SUFFIX,
                            "reasoning",
                            new MessageContent.Text(reasoning),
                            null,
                            null));
        }
        if (!text.isEmpty() || !toolCalls.isEmpty()) {
            messages.add(
                    new AguiMessage(
                            currentReplyId,
                            "assistant",
                            text.isEmpty() ? null : new MessageContent.Text(text),
                            toolCalls.isEmpty() ? null : toolCalls,
                            null));
        }
        resetReply();
    }

    private void resetReply() {
        currentReplyId = null;
        currentReasoning.setLength(0);
        currentText.setLength(0);
        currentToolCalls.clear();
    }

    /**
     * Emits any tool results that were started but never ended (defensive close on run end).
     */
    private void flushToolResults() {
        for (ToolResultBuffer buffer : toolResults.values()) {
            if (!buffer.isEmpty()) {
                messages.add(buffer.toToolMessage());
            }
        }
        toolResults.clear();
    }

    private static List<Msg> decodedInput(CustomEvent customEvent) {
        return JsonUtils.getJsonCodec()
                .fromJson(
                        JsonUtils.getJsonCodec().toJson(customEvent.getValue().get("input")),
                        new TypeReference<>() {
                        });
    }

    /**
     * A tool call of the current reply, accumulated from TOOL_CALL_START/DELTA/END.
     */
    private static final class ToolCallBuffer {
        private String name;
        private final StringBuilder arguments = new StringBuilder();
        private boolean complete;

        void setName(String name) {
            // Deltas often stream under a placeholder name (__fragment__); keep the real name
            // recorded on TOOL_CALL_START/END.
            if (name != null && !name.isBlank() && !"__fragment__".equals(name)) {
                this.name = name;
            }
        }

        void appendArguments(String delta) {
            if (delta != null && !delta.isEmpty()) {
                arguments.append(delta);
            }
        }

        void complete() {
            complete = true;
        }

        boolean isComplete() {
            return complete;
        }

        boolean hasArguments() {
            return arguments.length() > 0;
        }

        AguiToolCall toAguiToolCall(String toolCallId) {
            return new AguiToolCall(
                    toolCallId,
                    new AguiFunctionCall(name != null ? name : "unknown", arguments.toString()));
        }
    }

    /**
     * Tool result text/data accumulated from TOOL_RESULT_START/DELTA/END.
     */
    private static final class ToolResultBuffer {
        private final String replyId;
        private final String toolCallId;
        private final StringBuilder content = new StringBuilder();

        ToolResultBuffer(String replyId, String toolCallId) {
            this.replyId = replyId;
            this.toolCallId = toolCallId;
        }

        void appendText(String text) {
            if (text != null && !text.isEmpty()) {
                if (content.length() > 0) {
                    content.append("\n");
                }
                content.append(text);
            }
        }

        void appendData(ContentBlock data) {
            if (data == null) {
                return;
            }
            if (content.length() > 0) {
                content.append("\n");
            }
            if (data instanceof TextBlock textBlock) {
                content.append(textBlock.getText());
            } else {
                content.append(JsonUtils.getJsonCodec().toJson(data));
            }
        }

        boolean isEmpty() {
            return content.length() == 0;
        }

        AguiMessage toToolMessage() {
            return new AguiMessage(
                    replyId + ":" + toolCallId,
                    "tool",
                    content.length() > 0 ? new MessageContent.Text(content.toString()) : null,
                    null,
                    toolCallId);
        }
    }
}
