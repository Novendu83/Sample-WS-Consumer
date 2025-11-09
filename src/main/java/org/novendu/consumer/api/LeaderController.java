package org.novendu.consumer.api;

import org.novendu.consumer.leader.LeaderElectionService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/leader")
public class LeaderController {

    private final LeaderElectionService leaderService;

    public LeaderController(LeaderElectionService leaderService) {
        this.leaderService = leaderService;
    }

    @GetMapping("/status")
    public Map<String, Object> leader() {
        return leaderService.getLeadershipStatus();
    }
}
