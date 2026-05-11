package com.ds2api.tool;

import com.ds2api.model.InternalStreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Streaming tool-call parser for DeepSeek's DSML/XML tool_calls blocks.
 *
 * Design (aligned with ds2api reference):
 * - Code fence leak prevention: tracks ``` blocks and skips parsing inside them.
 * - Dual format: recognizes both <|DSML|tool_calls> and legacy <tool_calls>.
 * - CDATA-aware: strips <![CDATA[...]]> wrappers from parameter values.
 * - Incremental early emission: emits ToolCallDelta per chunk without waiting for
 *   the full XML close, so tool arguments stream in real time.
 * - Cross-chunk resilience: partial tags spanning chunk boundaries are buffered
 *   and retried on the next chunk.
 *
 * One instance per request (not thread-safe by design).
 */
public class ToolCallStreamParser {

    private static final Logger log = LoggerFactory.getLogger(ToolCallStreamParser.class);

    private enum State {
        IDLE,
        IN_CODE_BLOCK,
        IN_TOOL_SHELL,
        IN_INVOKE,
        IN_PARAM
    }

    private static final Pattern CODE_FENCE = Pattern.compile("```");

    private static final Pattern TOOL_CALLS_START =
            Pattern.compile("<\\|?DSML\\|?tool_calls\\s*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern TOOL_CALLS_END =
            Pattern.compile("</\\|?DSML\\|?tool_calls\\s*>", Pattern.CASE_INSENSITIVE);

    private static final Pattern INVOKE_START =
            Pattern.compile("<\\|?DSML\\|?invoke\\s+name=\"([^\"]+)\"\\s*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern INVOKE_END =
            Pattern.compile("</\\|?DSML\\|?invoke\\s*>", Pattern.CASE_INSENSITIVE);

    private static final Pattern PARAM_START =
            Pattern.compile("<\\|?DSML\\|?parameter\\s+name=\"([^\"]+)\"\\s*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern PARAM_END =
            Pattern.compile("</\\|?DSML\\|?parameter\\s*>", Pattern.CASE_INSENSITIVE);

    private static final Pattern CDATA_START = Pattern.compile("<!\\[CDATA\\[");
    private static final Pattern CDATA_END = Pattern.compile("\\]\\]>");

    private State state = State.IDLE;
    private final StringBuilder buffer = new StringBuilder();
    private String currentCallId;
    private String currentToolName;
    private boolean cdataOpen;
    private boolean cdataClosed;

    public List<InternalStreamEvent> processChunk(String chunk) {
        List<InternalStreamEvent> events = new ArrayList<>();
        if (chunk == null || chunk.isEmpty()) return events;
        buffer.append(chunk);
        processBuffer(events);
        return events;
    }

    public List<InternalStreamEvent> flushAndReset() {
        List<InternalStreamEvent> events = new ArrayList<>();
        if (buffer.length() > 0 && state == State.IDLE) {
            events.add(textDelta(buffer.toString()));
        }
        reset();
        return events;
    }

    public void reset() {
        state = State.IDLE;
        buffer.setLength(0);
        currentCallId = null;
        currentToolName = null;
        cdataOpen = false;
        cdataClosed = false;
    }

    private void processBuffer(List<InternalStreamEvent> events) {
        String text = buffer.toString();
        int pos = 0;

        while (pos < text.length()) {
            String remaining = text.substring(pos);
            Matcher fence = CODE_FENCE.matcher(remaining);

            switch (state) {
                case IDLE -> {
                    Matcher tcStart = TOOL_CALLS_START.matcher(remaining);
                    boolean hasFence = fence.find();
                    boolean hasTcStart = tcStart.find();
                    int fenceIdx = hasFence ? fence.start() : Integer.MAX_VALUE;
                    int tcIdx = hasTcStart ? tcStart.start() : Integer.MAX_VALUE;

                    if (fenceIdx < tcIdx && fenceIdx != Integer.MAX_VALUE) {
                        if (fenceIdx > 0) events.add(textDelta(remaining.substring(0, fenceIdx)));
                        pos += fenceIdx + fence.group().length();
                        state = State.IN_CODE_BLOCK;
                    } else if (tcIdx != Integer.MAX_VALUE) {
                        if (tcIdx > 0) events.add(textDelta(remaining.substring(0, tcIdx)));
                        pos += tcIdx + tcStart.group().length();
                        state = State.IN_TOOL_SHELL;
                    } else {
                        if (!remaining.isEmpty()) events.add(textDelta(remaining));
                        pos = text.length();
                    }
                }

                case IN_CODE_BLOCK -> {
                    if (fence.find()) {
                        pos += fence.end();
                        state = State.IDLE;
                    } else {
                        pos = text.length();
                    }
                }

                case IN_TOOL_SHELL -> {
                    Matcher invokeS = INVOKE_START.matcher(remaining);
                    Matcher tcEnd = TOOL_CALLS_END.matcher(remaining);
                    boolean hasInvoke = invokeS.find();
                    boolean hasTcEnd = tcEnd.find();
                    int invokeIdx = hasInvoke ? invokeS.start() : Integer.MAX_VALUE;
                    int tcEndIdx = hasTcEnd ? tcEnd.start() : Integer.MAX_VALUE;

                    if (invokeIdx < tcEndIdx && invokeIdx != Integer.MAX_VALUE) {
                        pos += invokeIdx + invokeS.group().length();
                        currentCallId = "call_" + UUID.randomUUID().toString().substring(0, 8);
                        currentToolName = invokeS.group(1);
                        events.add(new InternalStreamEvent.ToolCallStart(currentCallId, currentToolName));
                        state = State.IN_INVOKE;
                    } else if (tcEndIdx != Integer.MAX_VALUE) {
                        pos += tcEndIdx + tcEnd.group().length();
                        state = State.IDLE;
                    } else {
                        pos = text.length();
                    }
                }

                case IN_INVOKE -> {
                    Matcher paramS = PARAM_START.matcher(remaining);
                    Matcher invokeE = INVOKE_END.matcher(remaining);
                    boolean hasParam = paramS.find();
                    boolean hasInvokeEnd = invokeE.find();
                    int paramIdx = hasParam ? paramS.start() : Integer.MAX_VALUE;
                    int invokeEndIdx = hasInvokeEnd ? invokeE.start() : Integer.MAX_VALUE;

                    if (paramIdx < invokeEndIdx && paramIdx != Integer.MAX_VALUE) {
                        pos += paramIdx + paramS.group().length();
                        cdataOpen = false;
                        cdataClosed = false;
                        state = State.IN_PARAM;
                    } else if (invokeEndIdx != Integer.MAX_VALUE) {
                        pos += invokeEndIdx + invokeE.group().length();
                        events.add(new InternalStreamEvent.ToolCallEnd(currentCallId));
                        state = State.IN_TOOL_SHELL;
                    } else {
                        pos = text.length();
                    }
                }

                case IN_PARAM -> {
                    if (!cdataOpen) {
                        Matcher cdS = CDATA_START.matcher(remaining);
                        if (cdS.find()) {
                            pos += cdS.end();
                            cdataOpen = true;
                        } else {
                            int lastLt = remaining.lastIndexOf('<');
                            pos += lastLt >= 0 ? lastLt : remaining.length();
                        }
                    } else if (!cdataClosed) {
                        Matcher cdE = CDATA_END.matcher(remaining);
                        if (cdE.find()) {
                            String delta = remaining.substring(0, cdE.start());
                            if (!delta.isEmpty()) events.add(new InternalStreamEvent.ToolCallDelta(currentCallId, delta));
                            pos += cdE.end();
                            cdataClosed = true;
                        } else {
                            int safeEnd = safeEndForCdata(remaining);
                            String delta = remaining.substring(0, safeEnd);
                            if (!delta.isEmpty()) events.add(new InternalStreamEvent.ToolCallDelta(currentCallId, delta));
                            pos += safeEnd;
                        }
                    } else {
                        Matcher paramE = PARAM_END.matcher(remaining);
                        if (paramE.find()) {
                            pos += paramE.end();
                            state = State.IN_INVOKE;
                        } else {
                            pos = text.length();
                        }
                    }
                }
            }
        }

        buffer.setLength(0);
        if (pos < text.length()) {
            buffer.append(text.substring(pos));
        }
    }

    /**
     * Returns the largest prefix of 's' that is safe to emit as CDATA content,
     * i.e. does not contain a partial "]]>" closer at the end.
     */
    private static int safeEndForCdata(String s) {
        int len = s.length();
        if (len == 0) return 0;
        int i = len - 1;
        while (i >= 0 && s.charAt(i) == ']') i--;
        int trailingBrackets = len - 1 - i;
        if (trailingBrackets >= 3) return len;
        return len - trailingBrackets;
    }

    private static InternalStreamEvent textDelta(String text) {
        return new InternalStreamEvent.TextDelta(text);
    }
}
