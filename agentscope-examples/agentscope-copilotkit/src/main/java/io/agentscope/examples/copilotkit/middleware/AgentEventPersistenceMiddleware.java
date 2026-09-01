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
package io.agentscope.examples.copilotkit.middleware;

import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.agui.model.RunAgentInput;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.CustomEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import io.agentscope.core.event.ThinkingBlockDeltaEvent;
import io.agentscope.core.event.ToolCallDeltaEvent;
import io.agentscope.core.event.ToolResultDataDeltaEvent;
import io.agentscope.core.event.ToolResultEndEvent;
import io.agentscope.core.event.ToolResultStartEvent;
import io.agentscope.core.event.ToolResultTextDeltaEvent;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.middleware.AgentInput;
import io.agentscope.core.middleware.MiddlewareBase;
import io.agentscope.core.util.JsonUtils;
import io.agentscope.examples.copilotkit.model.AgentMessage;
import io.agentscope.examples.copilotkit.model.AgentSession;
import io.agentscope.examples.copilotkit.repository.AgentMessageRepository;
import io.agentscope.examples.copilotkit.repository.AgentSessionRepository;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import reactor.core.publisher.Flux;

/**
 * Persists agent events after merging their incremental (delta) parts.
 *
 * <p>Streaming agents emit one event per chunk (e.g. {@code TEXT_BLOCK_DELTA},
 * {@code TOOL_RESULT_TEXT_DELTA}). Saving every delta verbatim would store fragmented
 * messages that can never be re-assembled into the complete content later. This middleware
 * therefore buffers the events of the whole run, merges the delta events that belong to the
 * same logical block/tool call into a single complete event, and only then persists the
 * merged event sequence to the repository.
 *
 * <p>Tool results that are {@linkplain ToolResultBlock#isSuspended() suspended} (waiting for
 * external execution) are never persisted — neither their result events nor the
 * corresponding {@code TOOL_RESULT_START} lifecycle events.
 */
public class AgentEventPersistenceMiddleware implements MiddlewareBase {
    private final AgentMessageRepository agentMessageRepository;
    private final AgentSessionRepository agentSessionRepository;
    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(r -> new Thread(r, "agui-event-writer"));

    public AgentEventPersistenceMiddleware(
            AgentMessageRepository agentMessageRepository,
            AgentSessionRepository agentSessionRepository) {
        this.agentMessageRepository = agentMessageRepository;
        this.agentSessionRepository = agentSessionRepository;
    }

    @Override
    public Flux<AgentEvent> onAgent(
            Agent agent,
            RuntimeContext ctx,
            AgentInput input,
            Function<AgentInput, Flux<AgentEvent>> next) {
        RunAgentInput runAgentInput = ctx.get(RunAgentInput.class);
        if (!input.msgs().isEmpty()) {
            Msg msg = input.msgs().get(input.msgs().size() - 1);
            executor.execute(
                    () -> {
                        AgentSession agentSession = new AgentSession();
                        agentSession.setAgentId(agent.getAgentId());
                        agentSession.setThreadId(
                                Optional.ofNullable(runAgentInput)
                                        .map(RunAgentInput::getThreadId)
                                        .orElse(null));
                        agentSession.setUserId(ctx.getUserId());
                        agentSession.setName(msg.getTextContent());
                        agentSessionRepository.save(agentSession);
                        AgentMessage agentMessage = new AgentMessage();
                        agentMessage.setId(UUID.randomUUID().toString().replace("-", ""));
                        agentMessage.setThreadId(ctx.getSessionId());
                        agentMessage.setRunId(
                                Optional.ofNullable(runAgentInput)
                                        .map(RunAgentInput::getRunId)
                                        .orElse(null));
                        agentMessage.setUserId(ctx.getUserId());
                        agentMessage.setRawEvent(
                                JsonUtils.getJsonCodec()
                                        .toJson(
                                                new CustomEvent(
                                                        "agui.user.input", Map.of("input", input.msgs()))));
                        agentMessage.setCreateTime(LocalDateTime.now());
                        agentMessageRepository.save(agentMessage);
                    });
        }
        // Buffer the whole run, merge incremental events at the end, then persist once.
        // No event is dropped: lifecycle/terminal events and non-mergeable deltas are kept
        // verbatim; only the delta fragments of the same logical block/tool call are folded
        // into a single complete event (which still carries the full content).
        List<AgentEvent> buffered = new ArrayList<>();
        return next.apply(input)
                .doOnNext(buffered::add)
                .doFinally(
                        signal -> executor.execute(() -> persistRun(ctx, runAgentInput, buffered)));
    }

    private void persistRun(
            RuntimeContext ctx, RunAgentInput runAgentInput, List<AgentEvent> buffered) {
        if (buffered == null || buffered.isEmpty()) {
            return;
        }
        List<AgentEvent> events =
                mergeIncrementals(excludeSuspendedToolRuns(new ArrayList<>(buffered)));
        String threadId = ctx.getSessionId();
        String userId = ctx.getUserId();
        String runId = Optional.ofNullable(runAgentInput).map(RunAgentInput::getRunId).orElse(null);
        LocalDateTime createTime = LocalDateTime.now();
        // Spread createTime by nanoseconds so the repository's create-time ordering matches
        // the merged event order even within one batch.
        for (int i = 0; i < events.size(); i++) {
            AgentEvent agentEvent = events.get(i);
            AgentMessage agentMessage = new AgentMessage();
            agentMessage.setId(agentEvent.getId());
            agentMessage.setThreadId(threadId);
            agentMessage.setRunId(runId);
            agentMessage.setUserId(userId);
            agentMessage.setRawEvent(JsonUtils.getJsonCodec().toJson(agentEvent));
            agentMessage.setCreateTime(createTime.plusNanos(i));
            agentMessageRepository.save(agentMessage);
        }
    }

    /**
     * Drops the whole event trail of tool calls whose result is
     * {@linkplain ToolResultBlock#isSuspended() suspended}: the start event, all deltas and
     * the end event. The suspended marker only travels on the delta events (via event
     * metadata), so the suspended tool ids are collected first and the entire run is then
     * filtered in a second pass.
     */
    private List<AgentEvent> excludeSuspendedToolRuns(List<AgentEvent> events) {
        Set<String> suspendedToolCallIds = new HashSet<>();
        for (AgentEvent event : events) {
            if (isSuspendedToolResult(event)) {
                suspendedToolCallIds.add(toolResultToolCallId(event));
            }
        }
        if (suspendedToolCallIds.isEmpty()) {
            return events;
        }
        return events.stream()
                .filter(
                        event -> {
                            String toolCallId = toolResultToolCallId(event);
                            return toolCallId == null || !suspendedToolCallIds.contains(toolCallId);
                        })
                .toList();
    }

    /**
     * Merges the incremental events of one run into complete events.
     *
     * <p>Delta fragments that accumulate text ({@code TEXT_BLOCK_DELTA},
     * {@code THINKING_BLOCK_DELTA}, {@code TOOL_CALL_DELTA},
     * {@code TOOL_RESULT_TEXT_DELTA}) are concatenated per logical block/tool call and the
     * group is replaced by a single event carrying the full text, placed at the position of
     * the group's first fragment. All other events (start/end lifecycle, terminal events,
     * binary data deltas) are kept verbatim, so no logical event is lost.
     */
    private List<AgentEvent> mergeIncrementals(List<AgentEvent> events) {
        // mergeKey -> text accumulated from the group's delta fragments
        Map<String, StringBuilder> mergedTexts = new LinkedHashMap<>();
        for (AgentEvent event : events) {
            String delta = textDeltaOf(event);
            if (delta != null) {
                mergedTexts
                        .computeIfAbsent(mergeKeyOf(event), k -> new StringBuilder())
                        .append(delta);
            }
        }
        if (mergedTexts.isEmpty()) {
            return events;
        }
        List<AgentEvent> merged = new ArrayList<>(events.size());
        Set<String> emitted = new HashSet<>();
        for (AgentEvent event : events) {
            String mergeKey = mergeKeyOf(event);
            if (mergeKey == null) {
                merged.add(event);
                continue;
            }
            if (!emitted.add(mergeKey)) {
                continue; // fragments already folded into the representative event
            }
            merged.add(rebuildMergedDelta(event, mergedTexts.get(mergeKey).toString()));
        }
        return merged;
    }

    private String mergeKeyOf(AgentEvent event) {
        if (event instanceof TextBlockDeltaEvent delta) {
            return "text|" + delta.getReplyId() + "|" + delta.getBlockId();
        }
        if (event instanceof ThinkingBlockDeltaEvent delta) {
            return "thinking|" + delta.getReplyId() + "|" + delta.getBlockId();
        }
        if (event instanceof ToolCallDeltaEvent delta) {
            return "toolCall|" + delta.getReplyId() + "|" + delta.getToolCallId();
        }
        if (event instanceof ToolResultTextDeltaEvent delta) {
            return "toolResult|" + delta.getReplyId() + "|" + delta.getToolCallId();
        }
        return null;
    }

    private String textDeltaOf(AgentEvent event) {
        if (event instanceof TextBlockDeltaEvent delta) {
            return delta.getDelta();
        }
        if (event instanceof ThinkingBlockDeltaEvent delta) {
            return delta.getDelta();
        }
        if (event instanceof ToolCallDeltaEvent delta) {
            return delta.getDelta();
        }
        if (event instanceof ToolResultTextDeltaEvent delta) {
            return delta.getDelta();
        }
        return null;
    }

    /**
     * Rebuilds the delta event of the logical block/tool call with the complete merged text,
     * preserving the identity (id/createdAt/metadata/source) of its first fragment.
     */
    private AgentEvent rebuildMergedDelta(AgentEvent first, String fullText) {
        AgentEvent merged;
        if (first instanceof TextBlockDeltaEvent delta) {
            merged =
                    new TextBlockDeltaEvent(
                            delta.getId(),
                            delta.getCreatedAt(),
                            delta.getReplyId(),
                            delta.getBlockId(),
                            fullText);
        } else if (first instanceof ThinkingBlockDeltaEvent delta) {
            merged =
                    new ThinkingBlockDeltaEvent(
                            delta.getId(),
                            delta.getCreatedAt(),
                            delta.getReplyId(),
                            delta.getBlockId(),
                            fullText);
        } else if (first instanceof ToolCallDeltaEvent delta) {
            merged =
                    new ToolCallDeltaEvent(
                            delta.getId(),
                            delta.getCreatedAt(),
                            delta.getReplyId(),
                            delta.getToolCallId(),
                            delta.getToolCallName(),
                            fullText);
        } else if (first instanceof ToolResultTextDeltaEvent delta) {
            merged =
                    new ToolResultTextDeltaEvent(
                            delta.getId(),
                            delta.getCreatedAt(),
                            delta.getReplyId(),
                            delta.getToolCallId(),
                            delta.getToolCallName(),
                            fullText,
                            delta.getMetadata());
        } else {
            return first;
        }
        if (first.getSource() != null) {
            merged.withSource(first.getSource());
        }
        if (first.getMetadata() != null && !(first instanceof ToolResultTextDeltaEvent)) {
            merged.withMetadata(first.getMetadata());
        }
        return merged;
    }

    private boolean isSuspendedToolResult(AgentEvent event) {
        if (!(event instanceof ToolResultTextDeltaEvent
                || event instanceof ToolResultDataDeltaEvent)) {
            return false;
        }
        Map<String, Object> metadata = event.getMetadata();
        return metadata != null
                && Boolean.TRUE.equals(metadata.get(ToolResultBlock.METADATA_SUSPENDED));
    }

    /**
     * Returns the tool call id when the event belongs to the tool-result lifecycle.
     */
    private String toolResultToolCallId(AgentEvent event) {
        if (event instanceof ToolResultStartEvent start) {
            return start.getToolCallId();
        }
        if (event instanceof ToolResultTextDeltaEvent delta) {
            return delta.getToolCallId();
        }
        if (event instanceof ToolResultDataDeltaEvent delta) {
            return delta.getToolCallId();
        }
        if (event instanceof ToolResultEndEvent end) {
            return end.getToolCallId();
        }
        return null;
    }
}
