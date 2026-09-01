package io.agentscope.examples.copilotkit.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import org.springframework.data.annotation.CreatedDate;

@Entity
@Table(name = "tbl_agent_session")
public class AgentSession {
    @Id private String threadId;
    private String userId;
    private String name;
    private String agentId;

    @CreatedDate
    @Column(name = "create_time", updatable = false)
    private Instant createdAt;

    @Column(updatable = false)
    private Instant lastAccess;

    private boolean archived;

    public String getThreadId() {
        return threadId;
    }

    public void setThreadId(String threadId) {
        this.threadId = threadId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getLastAccess() {
        return lastAccess;
    }

    public void setLastAccess(Instant lastAccess) {
        this.lastAccess = lastAccess;
    }

    public boolean isArchived() {
        return archived;
    }

    public void setArchived(boolean archived) {
        this.archived = archived;
    }
}
