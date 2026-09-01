package io.agentscope.examples.copilotkit.repository;

import io.agentscope.examples.copilotkit.model.AgentSession;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AgentSessionRepository extends JpaRepository<AgentSession, String> {
    List<AgentSession> queryAllByUserIdEqualsOrderByCreatedAtDesc(String userId);
}
