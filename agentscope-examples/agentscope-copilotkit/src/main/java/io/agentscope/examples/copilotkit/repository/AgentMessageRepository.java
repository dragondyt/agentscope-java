package io.agentscope.examples.copilotkit.repository;

import io.agentscope.examples.copilotkit.model.AgentMessage;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AgentMessageRepository extends JpaRepository<AgentMessage, String> {
    List<AgentMessage> queryAllByUserIdAndThreadIdOrderByCreateTimeAsc(
            String userId, String threadId);
}
