package com.ds2api.compat;

import com.ds2api.client.DeepSeekFileClient;
import com.ds2api.config.ConfigLoaderService;
import com.ds2api.config.Ds2Config;
import com.ds2api.model.InternalRequest;
import com.ds2api.model.InternalRequest.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

/**
 * History-context splitting aligned with ds2api knowledge-base semantics.
 *
 * When current_input_file is enabled and the threshold is met, the full
 * conversation history (all messages except the latest) is merged and
 * uploaded as DS2API_HISTORY.txt, then replaced with a file-reference
 * message while keeping the latest user message at the end.
 */
@Service
public class HistoryFileSplitter {

    private static final Logger log = LoggerFactory.getLogger(HistoryFileSplitter.class);

    private final ConfigLoaderService configLoader;
    private final DeepSeekFileClient fileClient;

    public HistoryFileSplitter(ConfigLoaderService configLoader,
                               DeepSeekFileClient fileClient) {
        this.configLoader = configLoader;
        this.fileClient = fileClient;
    }

    /**
     * Apply history splitting if the config thresholds are met.
     * The token parameter comes from the runtime's account lease.
     *
     * @param req          The internal request (after prompt compat)
     * @param accountToken The account's bearer token for the file upload
     * @return Mono with potentially transformed request
     */
    public Mono<InternalRequest> applySplit(InternalRequest req, String accountToken) {
        Ds2Config config = configLoader.getConfig();
        Ds2Config.CurrentInputFileConfig cif = config.getCurrentInputFile();

        if (!cif.isEnabled()) {
            return Mono.just(req);
        }

        int threshold = cif.getThreshold();
        int totalChars = req.messages().stream()
            .mapToInt(m -> m.content() != null ? m.content().length() : 0)
            .sum();

        // threshold <= 0 means always trigger; >0 triggers only above the threshold
        if (threshold > 0 && totalChars < threshold) {
            return Mono.just(req);
        }
        if (req.messages().size() <= 1) {
            return Mono.just(req); // single-turn: nothing to split
        }

        log.info("[Split] Context length={}, threshold={}, triggering history file upload",
            totalChars, threshold);
        return splitAndUpload(req, accountToken);
    }

    private Mono<InternalRequest> splitAndUpload(InternalRequest req, String accountToken) {
        List<Message> msgs = req.messages();
        // All except the last message become history
        List<Message> history = msgs.subList(0, msgs.size() - 1);
        Message latestUser = msgs.get(msgs.size() - 1);

        StringBuilder historyText = new StringBuilder();
        for (Message m : history) {
            historyText.append("[").append(m.role()).append("]: ")
                       .append(m.content() != null ? m.content() : "")
                       .append("\n\n");
        }

        return fileClient.uploadHistoryFile(historyText.toString(), accountToken)
            .map(fileRef -> {
                List<Message> newMsgs = new ArrayList<>();
                newMsgs.add(new Message("user",
                    "[History context uploaded to file: " + fileRef
                    + "]\nPlease answer based on this file content."));
                newMsgs.add(latestUser);
                log.info("[Split] History replaced with file reference, keeping latest user msg");
                return req.withMessages(newMsgs);
            });
    }
}
