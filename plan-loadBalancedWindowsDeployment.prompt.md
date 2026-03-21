## Plan: Load-Balanced Windows Deployment

Prepare XTMon for deployment behind multiple Windows servers by treating the current app as a stateful Blazor Server-style application: keep Windows Authentication support through IIS and the load balancer, require sticky sessions for the UI, add shared Data Protection key persistence, and replace the replay-flow in-memory trigger with a farm-safe distributed or SQL-backed mechanism. This preserves the existing architecture where it is already safe, especially the SQL-coordinated JV worker, while removing the parts that are currently single-node only.

**Steps**
1. Phase 1: Confirm target deployment topology and scope boundaries. Decide whether the immediate target is a pragmatic active/passive or sticky-session active/active deployment, or a fully farm-safe active/active deployment. This decision blocks only the replay-flow processing changes; the IIS, Kerberos, and shared-key work can proceed regardless.
2. Phase 1: Preserve the current IIS plus Windows Authentication hosting model. Reuse the existing Negotiate/IIS configuration in [Program.cs](Program.cs#L70) and the IIS/SPN/delegation guidance in [README.md](README.md#L109). Treat IIS as the front-end host on each Windows node, with the external load balancer terminating or passing through HTTPS according to infrastructure standards.
3. Phase 1: Define load balancer requirements. Configure session affinity because the app uses interactive server rendering in [Program.cs](Program.cs#L79) and [Program.cs](Program.cs#L188), and the reconnect flow in [Components/Layout/ReconnectModal.razor.js](Components/Layout/ReconnectModal.razor.js#L31) assumes circuits may be lost if the request lands on another node. Enable WebSocket support and verify health probes do not break authenticated requests.
4. Phase 2: Add shared Data Protection key persistence in [Program.cs](Program.cs). Persist the key ring to a shared location accessible by all nodes, or to another common store supported by the hosting environment. This step is independent of the replay-flow refactor and should be implemented before farm testing because cross-node antiforgery and protected payload validation otherwise remain unreliable.
5. Phase 2: Keep the JV background worker unchanged except for deployment verification. [Data/JvCalculationProcessingService.cs](Data/JvCalculationProcessingService.cs#L47) already claims work through SQL using machine identity, so the plan should validate that the job stored procedures enforce single-worker ownership and tolerate multiple nodes polling concurrently.
6. Phase 3: Replace the singleton in-memory replay trigger with a farm-safe mechanism. The current queue in [Data/ReplayFlowProcessingQueue.cs](Data/ReplayFlowProcessingQueue.cs#L7), registration in [Program.cs](Program.cs#L129), worker in [Data/ReplayFlowProcessingService.cs](Data/ReplayFlowProcessingService.cs), and enqueue call in [Components/Pages/ReplayFlows.razor.cs](Components/Pages/ReplayFlows.razor.cs#L281) are the core active/active blocker. Preferred approach: move replay processing to database-backed polling or a durable distributed queue so any node can pick up submitted work, queued work survives node loss, and submission is no longer tied to the server that handled the UI action.
7. Phase 3: Decide whether replay processing should run on every web node or in a dedicated worker process. Recommended default: keep the web app responsible for submission and status UI, but move background replay execution to a single worker pattern or a DB-coordinated multi-worker pattern. This reduces operational ambiguity and removes dependence on web-node affinity for backend processing.
8. Phase 4: Update deployment documentation. Extend [README.md](README.md) with explicit load-balanced deployment guidance covering sticky sessions, shared Data Protection keys, WebSockets, node-local logging behavior, replay-flow farm limitations before the refactor, and the final supported topology after implementation.
9. Phase 4: Add environment and operations guidance. Document that self-log files currently write to local disk via [Program.cs](Program.cs#L21), so operators should expect per-node logs unless centralized logging is added. Confirm app pool identity, SPN registration, delegation, and SQL integrated-security behavior on all nodes.
10. Phase 5: Verify in stages. First verify single-node IIS deployment. Then verify two-node sticky-session deployment with shared keys. Finally verify replay submission, failover behavior, reconnect behavior, Kerberos authentication, and concurrent JV processing across nodes after the replay-flow redesign is complete.

**Relevant files**
- c:\Repos\VS\XTMon\Program.cs — hosting model, auth registration, interactive server rendering, hosted services, log path, and the future shared Data Protection configuration point.
- c:\Repos\VS\XTMon\Data\ReplayFlowProcessingQueue.cs — current in-memory per-node queue that prevents true farm-safe replay processing.
- c:\Repos\VS\XTMon\Data\ReplayFlowProcessingService.cs — background worker that currently depends on the in-memory queue.
- c:\Repos\VS\XTMon\Components\Pages\ReplayFlows.razor.cs — UI submission path that currently enqueues replay work locally.
- c:\Repos\VS\XTMon\Data\ReplayFlowRepository.cs — likely reuse point for converting replay processing to SQL-backed coordination.
- c:\Repos\VS\XTMon\Data\JvCalculationProcessingService.cs — example of an already database-coordinated worker pattern to reuse conceptually.
- c:\Repos\VS\XTMon\README.md — deployment document to update with supported load-balanced topology and required infrastructure settings.
- c:\Repos\VS\XTMon\appsettings.json — connection strings and any future deployment-specific options for shared storage or distributed processing.

**Verification**
1. Publish and deploy the app to two IIS nodes with the same app configuration and shared Data Protection keys, then confirm Windows Authentication succeeds through the load balancer using the intended DNS name and SPNs.
2. Validate session affinity by authenticating once, navigating between interactive pages, and confirming no forced reloads or circuit failures occur during normal use.
3. Force a reconnect scenario and verify the reconnect behavior described in [Components/Layout/ReconnectModal.razor.js](Components/Layout/ReconnectModal.razor.js#L31) behaves predictably with the chosen load balancer policy.
4. Submit replay-flow work repeatedly from either node and confirm processing still occurs if the submitting node is recycled or removed from rotation after the replay-flow redesign.
5. Submit JV jobs from multiple sessions and confirm only one node claims each job at a time while other nodes continue polling safely.
6. Confirm node-local logs, SQL logging, and IIS logs provide enough visibility to diagnose cross-node issues during rollout.

**Decisions**
- Included scope: deployment readiness for multiple Windows servers, IIS plus load balancer topology, Windows Authentication compatibility, shared key requirements, and replay/JV background processing implications.
- Deliberately excluded from this plan: changing the app away from interactive server rendering, redesigning user-facing UX, or introducing unrelated infrastructure such as container orchestration.
- Recommended topology: sticky-session load balancing for the UI plus shared Data Protection keys immediately, followed by replay-flow refactoring for true farm-safe backend processing.

**Further Considerations**
1. Replay processing architecture choice. Recommendation: database-backed polling if the team wants minimal new infrastructure; durable message queue if the team wants clearer separation and resilience.
2. Worker placement choice. Recommendation: dedicated worker process if operations prefers explicit ownership of background processing; otherwise a DB-coordinated web-node worker model is acceptable.
3. Rollout strategy. Recommendation: deploy shared keys and load balancer affinity first, then test, then refactor replay processing, then enable full multi-node production traffic.
