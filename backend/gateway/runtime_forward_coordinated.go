package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

var errSkippedRejectedRoute = errors.New("route skipped after response validation rejection")
var errSkippedNonRetryableRoute = errors.New("route retry skipped after deterministic upstream failure")

type coordinatedForwardRequest struct {
	c               *gin.Context
	path            string
	kind            protocolKind
	key             *storage.GatewayKey
	group           *storage.GatewayGroup
	body            []byte
	requestedModel  string
	stream          bool
	serviceTier     string
	reasoningEffort string
	thinkingEnabled bool
	routes          []storage.GatewayRoute
	validator       *responseValidator
	requestID       string
	firstToken      time.Duration
	hedgeActive     bool
	hedgeEligibilityKnown bool
	virtualCacheEligible  bool
	// hedgeTriggered is true only when the coordinator actually started an
	// auxiliary hedge attempt. A hedge-enabled group that completed on its
	// primary before the delay must not receive a virtual cache credit.
	hedgeTriggered     bool
	virtualCacheReason string
	affinity           routeAffinityContext
	prepareCache       *upstreamRequestPrepareCache
}

type coordinatedRoutePlan struct {
	Candidate          ScoredRoute
	TryOnRoute         int
	MaxTries           int
	ResponseMaxTries   int
}

func (p coordinatedRoutePlan) responseMaxTries() int {
	if p.ResponseMaxTries > 0 {
		return p.ResponseMaxTries
	}
	// Plans built before response_validation_retry_count was introduced only
	// carry MaxTries. Treat those in-memory plans as inheriting the transport
	// retry ladder, which preserves existing unit-test and caller behavior.
	return p.MaxTries
}

// coordinatedPlanScheduler keeps the configured hedge order while allowing a
// response-rule rejection to promote the same route's next retry into the
// next not-yet-started slot. Attempts that already started remain untouched;
// this preserves hedge concurrency without letting a new launch skip the
// current route's retry ladder.
type coordinatedPlanScheduler struct {
	mu          sync.Mutex
	entries     []coordinatedRoutePlan
	maxAttempts int
	started     int
}

func newCoordinatedPlanScheduler(plan []coordinatedRoutePlan, maxAttempts int) *coordinatedPlanScheduler {
	entries := append([]coordinatedRoutePlan(nil), plan...)
	if maxAttempts <= 0 || maxAttempts > len(entries) {
		maxAttempts = len(entries)
	}
	return &coordinatedPlanScheduler{entries: entries, maxAttempts: maxAttempts}
}

func (s *coordinatedPlanScheduler) reserve(number int) coordinatedRoutePlan {
	if s == nil || number <= 0 {
		return coordinatedRoutePlan{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if number > s.started {
		s.started = number
	}
	if number > len(s.entries) {
		return coordinatedRoutePlan{}
	}
	return s.entries[number-1]
}

func (s *coordinatedPlanScheduler) prioritizeRetry(attempt *coordinatedForwardAttempt) {
	if s == nil || attempt == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started >= s.maxAttempts {
		return
	}
	wantTry := attempt.Plan.TryOnRoute + 1
	if responseMaxTries := attempt.Plan.responseMaxTries(); responseMaxTries <= wantTry {
		return
	}
	for index := s.started; index < len(s.entries); index++ {
		entry := s.entries[index]
		if entry.Candidate.Route.ID != attempt.Route.ID || entry.TryOnRoute != wantTry {
			continue
		}
		s.entries[s.started], s.entries[index] = s.entries[index], s.entries[s.started]
		return
	}
}

func (s *coordinatedPlanScheduler) snapshot() []coordinatedRoutePlan {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	count := s.maxAttempts
	if count > len(s.entries) {
		count = len(s.entries)
	}
	return append([]coordinatedRoutePlan(nil), s.entries[:count]...)
}

type coordinatedForwardAttempt struct {
	Info       hedgeAttemptInfo
	Plan       coordinatedRoutePlan
	Route      storage.GatewayRoute
	Target     *upstreamTarget
	StartedAt  time.Time
	DurationMS int64

	UpstreamModel string
	MappingChain  string
	UpstreamKind  protocolKind
	UpstreamPath  string
	UpstreamURL   string
	Converted     bool
	ForwardBody   []byte
	UsageMeta     usageRecordMeta

	Status       int
	Headers      http.Header
	UpstreamBody []byte
	ClientBody   []byte
	Tokens       UsageTokens
	FirstTokenMS *int64
	Err          error
	ErrInfo      usageErrorInfo
	Terminal     bool
	Skipped      bool
	Validation   validationResult
	Recovery     bool

	Gate            *streamPrefixGateWriter
	Cancel          context.CancelFunc
	runnerDone      chan struct{}
	streamDone      chan streamAttemptResult
	streamMu        sync.Mutex
	streamResult    streamAttemptResult
	streamReady     bool
	gateCommitErr   error
	upstreamStarted atomic.Bool
}

func (a *coordinatedForwardAttempt) markUpstreamStarted() {
	if a != nil {
		a.upstreamStarted.Store(true)
	}
}

func (a *coordinatedForwardAttempt) didStartUpstream() bool {
	return a != nil && a.upstreamStarted.Load()
}

func (a *coordinatedForwardAttempt) streamControlSnapshot() (*streamPrefixGateWriter, context.CancelFunc, chan streamAttemptResult, bool) {
	if a == nil {
		return nil, nil, nil, false
	}
	a.streamMu.Lock()
	defer a.streamMu.Unlock()
	return a.Gate, a.Cancel, a.streamDone, a.streamReady
}

func (a *coordinatedForwardAttempt) cancelStreamLoser() {
	gate, cancel, _, _ := a.streamControlSnapshot()
	if gate != nil {
		gate.Lose()
	}
	if cancel != nil {
		cancel()
	}
}

func (a *coordinatedForwardAttempt) setGateCommitError(err error) {
	a.streamMu.Lock()
	a.gateCommitErr = err
	a.streamMu.Unlock()
}

func (a *coordinatedForwardAttempt) gateCommitError() error {
	a.streamMu.Lock()
	defer a.streamMu.Unlock()
	return a.gateCommitErr
}

func (a *coordinatedForwardAttempt) setTerminal(value bool) {
	a.streamMu.Lock()
	a.Terminal = value
	a.streamMu.Unlock()
}

func (a *coordinatedForwardAttempt) setStreamResult(result streamAttemptResult) {
	a.streamMu.Lock()
	a.streamResult = result
	a.streamReady = true
	a.streamMu.Unlock()
}

func (a *coordinatedForwardAttempt) awaitStreamResult() streamAttemptResult {
	_, _, streamDone, ready := a.streamControlSnapshot()
	if ready {
		a.streamMu.Lock()
		result := a.streamResult
		a.streamMu.Unlock()
		return result
	}
	if streamDone == nil {
		return streamAttemptResult{Err: context.Canceled}
	}
	result := <-streamDone
	a.setStreamResult(result)
	return result
}

func (rt *Runtime) shouldUseCoordinatedForward(group *storage.GatewayGroup, validator *responseValidator, request hedgeRequest) (bool, bool, bool) {
	validationEnabled := validator != nil && validator.Enabled()
	virtualCacheEligible := hedgeEligible(request)
	hedgeActive := group != nil && group.HedgeEnabled && virtualCacheEligible
	return validationEnabled || hedgeActive, hedgeActive, virtualCacheEligible
}

func (rt *Runtime) handleForwardCoordinated(req coordinatedForwardRequest) {
	if req.prepareCache == nil {
		req.prepareCache = &upstreamRequestPrepareCache{}
	}
	groupsByChannel := rt.loadGroupsByChannel(req.c.Request.Context(), req.routes)
	candidates := rt.sortRoutesWithAffinity(req.routes, groupsByChannel, req.group.RateSortDirection, time.Now(), nil, &req.affinity, req.requestedModel)
	candidates = rt.orderLoadBalancedCandidates(candidates, req.group, &req.affinity)
	if req.affinity.Recovery && req.hedgeActive {
		// A cooled route must complete its single recovery probe before any
		// concurrent hedge is launched; fallback resumes after that probe fails.
		req.hedgeActive = false
	}
	if len(candidates) == 0 {
		rt.finalizeUsageFailure(req.requestID, req.key)
		rt.writeGatewayError(req.c, req.kind, http.StatusServiceUnavailable, "api_error", "no schedulable routes")
		return
	}
	plan := buildCoordinatedRoutePlan(candidates, req.group, req.hedgeActive, req.validator != nil && req.validator.Enabled())
	if req.affinity.Recovery {
		plan = removeRecoveryRouteRetries(plan, req.affinity.RecoveryRouteID)
	}
	if len(plan) == 0 {
		rt.finalizeUsageFailure(req.requestID, req.key)
		rt.writeGatewayError(req.c, req.kind, http.StatusServiceUnavailable, "api_error", "no attempts available")
		return
	}

	maxAttempts := len(plan)
	if req.hedgeActive {
		maxAttempts = minInt(maxAttempts, hedgePolicy{MaxAttempts: req.group.HedgeMaxAttempts}.normalized().MaxAttempts)
	}
	planScheduler := newCoordinatedPlanScheduler(plan, maxAttempts)
	policy := hedgePolicy{
		Enabled:     req.hedgeActive,
		Delay:       time.Duration(req.group.HedgeDelaySeconds * float64(time.Second)),
		MaxParallel: req.group.HedgeMaxParallel,
		MaxAttempts: planScheduler.maxAttempts,
	}
	if policy.MaxParallel <= 0 {
		policy.MaxParallel = defaultHedgeMaxParallel
	}
	if policy.MaxParallel > policy.MaxAttempts {
		policy.MaxParallel = policy.MaxAttempts
	}

	var states sync.Map
	var excludedRoutes sync.Map
	var responseRejectedRoutes sync.Map
	var retrySuppressedRoutes sync.Map
	run := func(ctx context.Context, info hedgeAttemptInfo) (*coordinatedForwardAttempt, error) {
		entry := planScheduler.reserve(info.Number)
		attempt := &coordinatedForwardAttempt{
			Info: info, Plan: entry, Route: entry.Candidate.Route, StartedAt: time.Now(),
			runnerDone: make(chan struct{}),
		}
		if info.Number == 1 && req.affinity.Recovery && req.affinity.RecoveryRouteID == attempt.Route.ID {
			attempt.Recovery = true
		}
		states.Store(info.Number, attempt)
		defer close(attempt.runnerDone)
		if attempt.Plan.TryOnRoute > 0 {
			if _, suppressed := retrySuppressedRoutes.Load(attempt.Route.ID); suppressed {
				attempt.Skipped = true
				attempt.Err = errSkippedNonRetryableRoute
				return attempt, attempt.Err
			}
		}
		if _, excluded := excludedRoutes.Load(attempt.Route.ID); excluded {
			// Preparation/configuration failures are hard route exclusions. They
			// must not be repeated through the same-route retry ladder.
			attempt.Skipped = true
			attempt.Err = errSkippedNonRetryableRoute
			return attempt, attempt.Err
		}
		if _, rejected := responseRejectedRoutes.Load(attempt.Route.ID); rejected {
			responseMaxTries := attempt.Plan.responseMaxTries()
			if attempt.Plan.TryOnRoute > 0 && attempt.Plan.TryOnRoute < responseMaxTries {
				// A response-rule rejection excludes a route's primary entry, but it
				// must not cancel the explicitly planned same-route retry ladder.
			} else {
				// Once the response-rule retry budget is exhausted, skip remaining
				// entries for this route so the scheduler can move to failover.
				attempt.Skipped = true
				attempt.Err = errSkippedRejectedRoute
				return attempt, attempt.Err
			}
		}
		if err := rt.prepareCoordinatedAttempt(&req, attempt); err != nil {
			attempt.Err = err
			attempt.ErrInfo = usageErrorInfo{
				Type: "config", Summary: err.Error(),
				Detail: fmt.Sprintf("config error\nroute_id: %d\nsource_kind: %s\nerror: %s\n", attempt.Route.ID, attempt.Route.NormalizeSourceKind(), err.Error()),
			}
			attempt.DurationMS = time.Since(attempt.StartedAt).Milliseconds()
			// Preparation errors are route-local and are not useful to retry on the
			// same route. They may move to another route only when the legacy
			// transport failover policy allows it; response validation alone must
			// not turn a configuration error into a route switch.
			excludedRoutes.Store(attempt.Route.ID, struct{}{})
			if !req.hedgeActive && !coordinatedTransportFailoverEnabled(req.group) {
				return attempt, stopHedgeAttempts(err)
			}
			return attempt, err
		}
		attemptTimeout := coordinatedAttemptFirstTokenTimeout(
			req.firstToken, req.group, req.hedgeActive, planScheduler.snapshot(), info.Number, attempt.Route.ID,
		)
		var runErr error
		if req.stream {
			_, runErr = rt.runCoordinatedStreamAttempt(ctx, &req, attempt, attemptTimeout)
		} else {
			_, runErr = rt.runCoordinatedNonStreamAttempt(ctx, &req, attempt, attemptTimeout)
		}
		suppressSameRouteRetry := coordinatedAttemptSuppressesSameRouteRetries(attempt)
		if suppressSameRouteRetry {
			retrySuppressedRoutes.Store(attempt.Route.ID, struct{}{})
		}
		// Response validation is allowed to switch routes even when the legacy
		// transport failover toggle is off. Ordinary transport/status failures
		// must retain the original retry/failover semantics, however; make the
		// first such result terminal so the coordinator does not accidentally
		// treat the validation route budget as a transport failover budget.
		if !req.hedgeActive && !coordinatedTransportFailoverEnabled(req.group) {
			validation, status, _, terminal := attempt.validationSnapshot()
			if !validation.IsRejected() {
				failed := runErr != nil || terminal || status < 200 || status >= 300
				if failed && (suppressSameRouteRetry || !coordinatedSameRouteRetryAllowed(req.group, attempt)) {
					attempt.setTerminal(true)
					if runErr == nil {
						runErr = fmt.Errorf("upstream status %d", status)
					}
					return attempt, stopHedgeAttempts(runErr)
				}
			}
		}
		return attempt, runErr
	}
	validate := func(attempt *coordinatedForwardAttempt) (bool, error) {
		accepted, err := validateCoordinatedAttempt(attempt, &excludedRoutes, &responseRejectedRoutes)
		if !accepted && attempt != nil {
			validation, _, _, _ := attempt.validationSnapshot()
			if validation.IsRejected() && !validation.PostCommit {
				planScheduler.prioritizeRetry(attempt)
			}
		}
		return accepted, err
	}
	hooks := hedgeHooks[*coordinatedForwardAttempt]{
			OnWinner: func(result hedgeAttemptResult[*coordinatedForwardAttempt]) {
				if result.Value != nil {
					req.virtualCacheReason = rt.virtualCacheReasonForWinner(&req, result.Value, &states)
					gate, _, _, _ := result.Value.streamControlSnapshot()
					if gate != nil {
						if req.virtualCacheReason != "" {
							percent := 100
							if req.virtualCacheReason == storage.GatewayVirtualCacheReasonProviderGlobal && result.Value.Target != nil {
								percent, _ = ProviderVirtualCachePercentForModel(result.Value.Target.Provider, result.Value.UpstreamModel)
							}
							gate.EnableVirtualCachePercent(req.kind, percent)
						}
					result.Value.setGateCommitError(gate.Win())
				}
			}
		},
		OnCanceled: func(info hedgeAttemptInfo) {
			value, ok := states.Load(info.Number)
			if !ok {
				return
			}
			attempt, ok := value.(*coordinatedForwardAttempt)
			if !ok || attempt == nil {
				return
			}
			// Cancellation is deliberately issued even while the runner is still
			// unwinding. The attempt owns an independent context for stream work;
			// waiting for runnerDone here would leave cancellation-resistant losers
			// running past the hedge coordinator's cleanup window.
			attempt.cancelStreamLoser()
		},
	}

	result, runErr := runHedge(req.c.Request.Context(), req.hedgeActive, policy, run, validate, hooks)
	req.hedgeTriggered = req.virtualCacheReason == storage.GatewayVirtualCacheReasonHedge
	if result.Winner != nil && result.Winner.Value != nil && req.stream {
		winner := result.Winner.Value
		streamResult := winner.awaitStreamResult()
		winner.applyStreamResult(streamResult)
	}
	if req.stream {
		rt.cleanupCoordinatedStreamLosers(result, &states)
	}
	winnerUsageID := rt.auditCoordinatedAttempts(&req, planScheduler.snapshot(), result, &states)
	if result.Winner == nil || result.Winner.Value == nil {
		rt.finalizeUsageFailure(req.requestID, req.key)
		rt.writeCoordinatedFailure(&req, result, runErr)
		return
	}
	winner := result.Winner.Value
	if req.stream {
		rt.finishCoordinatedStream(&req, winner, winnerUsageID)
		return
	}
	rt.finishCoordinatedNonStream(&req, winner, winnerUsageID)
}

func buildCoordinatedRoutePlan(candidates []ScoredRoute, group *storage.GatewayGroup, hedgeActive, validationEnabled bool) []coordinatedRoutePlan {
	if len(candidates) == 0 || group == nil {
		return nil
	}
	routeLimit := 1
	if hedgeActive {
		maxAttempts := hedgePolicy{MaxAttempts: group.HedgeMaxAttempts}.normalized().MaxAttempts
		routeLimit = minInt(len(candidates), maxAttempts)
	} else if validationEnabled {
		// A pre-commit response-rule match is an explicit route-switch signal.
		// It must not be gated by the ordinary transport failover toggle. The
		// configured same-route retry ladder is still honored before switching.
		// A positive failover_max is
		// still honored as the operator's route budget; zero means all available
		// routes for validation so enabling a rule cannot silently become a
		// no-op on groups that disabled transport failover.
		if group.FailoverMax > 0 {
			routeLimit = minInt(len(candidates), 1+group.FailoverMax)
		} else {
			routeLimit = len(candidates)
		}
	} else if group.RetryEnabled && group.FailoverEnabled {
		failoverMax := group.FailoverMax
		if failoverMax < 0 {
			failoverMax = 0
		}
		routeLimit = minInt(len(candidates), 1+failoverMax)
	}
	transportRetries := 0
	if group.RetryEnabled {
		transportRetries = maxInt(group.RetryCount, 0)
	}
	responseRetries := transportRetries
	if validationEnabled {
		responseRetries = effectiveResponseValidationRetryCount(group)
	}
	planRetries := maxInt(transportRetries, responseRetries)
	plan := make([]coordinatedRoutePlan, 0, routeLimit*(1+planRetries))
	if hedgeActive {
		// Keep distinct routes in the initial hedge rounds. If a response rule
		// rejects a route, the coordinator promotes that route's next retry into
		// the next unstarted slot without disturbing already-running hedges.
		for try := 0; try <= planRetries; try++ {
			for routeIndex := 0; routeIndex < routeLimit; routeIndex++ {
				plan = append(plan, coordinatedRoutePlan{
					Candidate: candidates[routeIndex], TryOnRoute: try,
					MaxTries: 1 + transportRetries, ResponseMaxTries: 1 + responseRetries,
				})
			}
		}
		return plan
	}
	for routeIndex := 0; routeIndex < routeLimit; routeIndex++ {
		for try := 0; try <= planRetries; try++ {
			plan = append(plan, coordinatedRoutePlan{
				Candidate: candidates[routeIndex], TryOnRoute: try,
				MaxTries: 1 + transportRetries, ResponseMaxTries: 1 + responseRetries,
			})
		}
	}
	return plan
}

func coordinatedTransportFailoverEnabled(group *storage.GatewayGroup) bool {
	return group != nil && group.RetryEnabled && group.FailoverEnabled && group.FailoverMax > 0
}

func coordinatedSameRouteRetryAllowed(group *storage.GatewayGroup, attempt *coordinatedForwardAttempt) bool {
	return group != nil && group.RetryEnabled && attempt != nil && attempt.Plan.TryOnRoute+1 < attempt.Plan.MaxTries
}

func effectiveResponseValidationRetryCount(group *storage.GatewayGroup) int {
	if group == nil || !group.RetryEnabled {
		return 0
	}
	retries := group.ResponseValidationRetryCount
	if retries < 0 {
		retries = group.RetryCount
	}
	if retries < 0 {
		return 0
	}
	if retries > 10 {
		return 10
	}
	return retries
}

// coordinatedAttemptFirstTokenTimeout keeps the legacy timeout contract when
// response validation has expanded the route plan: a first-token timeout is a
// transport failover trigger, not a reason to switch routes solely for regex
// validation. Hedge attempts retain their independent timeout ladder.
func coordinatedAttemptFirstTokenTimeout(
	configured time.Duration,
	group *storage.GatewayGroup,
	hedgeActive bool,
	plan []coordinatedRoutePlan,
	number int,
	routeID uint,
) time.Duration {
	if configured <= 0 {
		return 0
	}
	if hedgeActive {
		if number > 0 && number < len(plan) {
			return configured
		}
		return 0
	}
	if !coordinatedTransportFailoverEnabled(group) || number <= 0 {
		return 0
	}
	for index := number; index < len(plan); index++ {
		if plan[index].Candidate.Route.ID != routeID {
			return configured
		}
	}
	return 0
}

func validateCoordinatedAttempt(attempt *coordinatedForwardAttempt, excludedRoutes, responseRejectedRoutes *sync.Map) (bool, error) {
	if attempt == nil || attempt.Skipped {
		return false, errSkippedRejectedRoute
	}
	if excludedRoutes != nil {
		if _, excluded := excludedRoutes.Load(attempt.Route.ID); excluded {
			// Preparation/configuration failures are hard route exclusions and
			// cannot be retried on the same route.
			return false, errSkippedNonRetryableRoute
		}
	}
	if responseRejectedRoutes != nil {
		if _, rejected := responseRejectedRoutes.Load(attempt.Route.ID); rejected {
			responseMaxTries := attempt.Plan.responseMaxTries()
			if attempt.Plan.TryOnRoute == 0 || attempt.Plan.TryOnRoute >= responseMaxTries {
				// A same-route retry may already be running when another attempt on
				// that route matches a pre-commit response rule. Only the explicitly
				// configured response retry ladder remains eligible.
				return false, errSkippedRejectedRoute
			}
		}
	}
	validation, status, _, terminal := attempt.validationSnapshot()
	if validation.IsRejected() && !validation.PostCommit {
		if responseRejectedRoutes != nil {
			responseRejectedRoutes.Store(attempt.Route.ID, struct{}{})
		}
		return false, &responseRejectedError{Result: validation}
	}
	return (status >= 200 && status < 300) || terminal || validation.PostCommit, nil
}

func (rt *Runtime) prepareCoordinatedAttempt(req *coordinatedForwardRequest, attempt *coordinatedForwardAttempt) error {
	target, err := rt.resolveUpstreamTarget(&attempt.Route)
	if err != nil {
		return err
	}
	attempt.Target = target
	target.onUpstreamStart = attempt.markUpstreamStarted
	rt.applyRouteUserAgent(target, req.group, &attempt.Route)
	groupMapping := ParseModelMapping(req.group.ModelMappingJSON)
	routeMapping := ParseModelMapping(attempt.Route.ModelMappingJSON)
	attempt.UpstreamModel, attempt.MappingChain = ResolveModel(req.requestedModel, routeMapping, groupMapping)
	if attempt.UpstreamModel == "" {
		attempt.UpstreamModel = req.requestedModel
	}
	reasoningEffort := ApplyThinkingEnabledEffortFallback(
		req.reasoningEffort, req.thinkingEnabled, attempt.UpstreamModel, req.requestedModel,
	)
	routeProtocol := rt.normalizeUpstreamProtocol(attempt.Route.UpstreamProtocol)
	if attempt.Route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider &&
		routeProtocol == storage.GatewayUpstreamProtocolAuto && target.Provider != nil {
		if providerProtocol := rt.normalizeProviderProtocol(target.Provider.UpstreamProtocol); providerProtocol != storage.GatewayUpstreamProtocolAuto {
			routeProtocol = providerProtocol
		}
	}
	attempt.UpstreamKind = protocol.ResolveUpstream(routeProtocol, req.kind, attempt.UpstreamModel)
	if protocol.IsOpenAIFamily(req.kind) && strings.Contains(req.path, "/completions") && !strings.Contains(req.path, "/chat/") {
		if attempt.UpstreamKind == protocolAnthropic || attempt.UpstreamKind == protocol.KindOpenAIResponses {
			return errors.New("protocol conversion for /v1/completions is not supported; use /v1/chat/completions")
		}
		attempt.UpstreamKind = protocol.KindOpenAIChat
	}
	attempt.ForwardBody, attempt.UpstreamPath, attempt.Converted, err = req.prepareCache.prepare(
		rt.Service, req.body, req.kind, attempt.UpstreamKind,
		req.requestedModel, attempt.UpstreamModel, req.stream, req.path,
	)
	if err != nil {
		return fmt.Errorf("protocol convert failed: %w", err)
	}
	attempt.UpstreamURL = target.BaseURL + attempt.UpstreamPath
	attempt.UsageMeta = usageRecordMeta{
		InboundEndpoint: req.path, UpstreamEndpoint: attempt.UpstreamPath,
		InboundProtocol: string(req.kind), UpstreamProtocol: string(attempt.UpstreamKind),
		ProtocolConverted: attempt.Converted, ServiceTier: req.serviceTier,
		ReasoningEffort: reasoningEffort, UpstreamURL: attempt.UpstreamURL,
	}
	return nil
}

func (rt *Runtime) runCoordinatedNonStreamAttempt(ctx context.Context, req *coordinatedForwardRequest, attempt *coordinatedForwardAttempt, timeout time.Duration) (*coordinatedForwardAttempt, error) {
	if attempt.Target != nil {
		attempt.Target.onUpstreamStart = attempt.markUpstreamStarted
	}
	attempt.Status, attempt.Headers, attempt.UpstreamBody, attempt.FirstTokenMS, attempt.Err = rt.forwardOnce(
		ctx, nil, attempt.Target, attempt.UpstreamPath, req.c.Request.Method, req.c.Request.Header,
		attempt.ForwardBody, false, attempt.UpstreamKind, timeout,
	)
	attempt.DurationMS = time.Since(attempt.StartedAt).Milliseconds()
	if attempt.Status >= 400 {
		attempt.ClientBody = rt.convertErrorBody(attempt.UpstreamBody, req.kind, attempt.UpstreamKind, attempt.Converted)
	} else {
		attempt.ClientBody = rt.convertUpstreamResponse(attempt.UpstreamBody, req.kind, attempt.UpstreamKind, attempt.UpstreamModel, false, attempt.Converted)
		attempt.Tokens = rt.parseUsageByKind(attempt.UpstreamBody, false, attempt.UpstreamKind)
	}
	if attempt.Err == nil && len(attempt.ClientBody) > 0 {
		attempt.Validation = req.validator.Validate(attempt.ClientBody, attempt.Headers, string(req.kind), req.requestedModel)
		if attempt.Validation.IsRejected() {
			attempt.ErrInfo = validationErrorInfo(attempt.Validation)
			return attempt, nil
		}
	}
	if attempt.Err != nil || rt.isFailoverStatus(attempt.Status, req.group.FailoverOn4xx) {
		attempt.ErrInfo = rt.buildUpstreamErrorInfoCfg(rt.gatewayRuntime(), attempt.Err, attempt.Status, attempt.Headers, attempt.UpstreamBody, attempt.UpstreamURL, req.c.Request.Method)
		if attempt.Err == nil {
			attempt.Err = fmt.Errorf("upstream status %d: %s", attempt.Status, attempt.ErrInfo.Summary)
		}
		return attempt, attempt.Err
	}
	if attempt.Status >= 400 {
		attempt.Terminal = true
		attempt.ErrInfo = rt.buildUpstreamErrorInfoCfg(rt.gatewayRuntime(), nil, attempt.Status, attempt.Headers, attempt.UpstreamBody, attempt.UpstreamURL, req.c.Request.Method)
	}
	return attempt, nil
}

func (rt *Runtime) runCoordinatedStreamAttempt(ctx context.Context, req *coordinatedForwardRequest, attempt *coordinatedForwardAttempt, timeout time.Duration) (*coordinatedForwardAttempt, error) {
	if attempt.Target != nil {
		attempt.Target.onUpstreamStart = attempt.markUpstreamStarted
	}
	streamValidator := req.validator.NewStreamValidator(string(req.kind), req.requestedModel)
	gate := newStreamPrefixGateWriter(req.c.Writer, streamValidator)
	clone := req.c.Copy()
	clone.Writer = gate
	clone.Request = req.c.Request.Clone(req.c.Request.Context())
	upstreamBase := context.WithoutCancel(req.c.Request.Context())
	upstreamCtx, cancel := context.WithCancel(upstreamBase)
	streamDone := make(chan streamAttemptResult, 1)
	attempt.streamMu.Lock()
	attempt.Gate = gate
	attempt.Cancel = cancel
	attempt.streamDone = streamDone
	attempt.streamMu.Unlock()
	go func() {
		result := rt.forwardStream(
			withPreservedAttemptCancel(upstreamCtx), clone, attempt.Target, attempt.UpstreamPath,
			req.c.Request.Method, req.c.Request.Header, attempt.ForwardBody,
			req.kind, attempt.UpstreamKind, attempt.UpstreamModel, attempt.Converted, timeout,
		)
		clientBody := []byte(nil)
		validation := result.ValidationRejection
		errInfo := usageErrorInfo{}
		terminal := false
		if result.Status >= 400 && len(result.Body) > 0 {
			clientBody = rt.convertErrorBody(result.Body, req.kind, attempt.UpstreamKind, attempt.Converted)
			validation = req.validator.Validate(clientBody, result.Headers, string(req.kind), req.requestedModel)
		}
		if !validation.IsRejected() && (result.Err != nil || rt.isFailoverStatus(result.Status, req.group.FailoverOn4xx)) {
			errInfo = rt.buildUpstreamErrorInfoCfg(
				rt.gatewayRuntime(), result.Err, result.Status, result.Headers, result.Body,
				attempt.UpstreamURL, req.c.Request.Method,
			)
			if result.Err == nil {
				result.Err = fmt.Errorf("upstream status %d: %s", result.Status, errInfo.Summary)
			}
		} else if result.Status >= 400 {
			terminal = true
			errInfo = rt.buildUpstreamErrorInfoCfg(
				rt.gatewayRuntime(), nil, result.Status, result.Headers, result.Body,
				attempt.UpstreamURL, req.c.Request.Method,
			)
		}
		// Publish transport/error state before Finish can signal a pending gate.
		// Otherwise a final validation-accepted decision could win a race with a
		// first-token timeout or a pre-commit buffer failure.
		attempt.streamMu.Lock()
		attempt.applyStreamResultLocked(result)
		attempt.ClientBody = clientBody
		attempt.Validation = validation
		attempt.ErrInfo = errInfo
		attempt.Terminal = terminal
		attempt.streamMu.Unlock()
		final := gate.Finish()
		if final.IsRejected() && !final.PostCommit {
			result.ValidationRejection = final
			validation = final
		}
		if late := gate.LateMatch(); late.IsRejected() {
			result.PostCommitValidation = late
			validation = late
		}
		attempt.streamMu.Lock()
		attempt.applyStreamResultLocked(result)
		attempt.ClientBody = clientBody
		attempt.Validation = validation
		attempt.ErrInfo = errInfo
		attempt.Terminal = terminal
		attempt.streamResult = result
		attempt.streamReady = true
		attempt.streamMu.Unlock()
		streamDone <- result
	}()

	select {
	case decision := <-gate.Ready():
		attempt.streamMu.Lock()
		if attempt.Status == 0 {
			attempt.Status = gate.Status()
			attempt.Headers = gate.Header().Clone()
		}
		if decision.IsRejected() {
			attempt.Validation = decision
		}
		rejected := attempt.Validation.IsRejected()
		status := attempt.Status
		attemptErr := attempt.Err
		attempt.streamMu.Unlock()
		if rejected {
			cancel()
			// Finish signals the validation decision before the forwarding
			// goroutine publishes its final stream result. Wait for that result
			// before the caller inspects attempt state or starts another route.
			streamResult := attempt.awaitStreamResult()
			attempt.applyStreamResult(streamResult)
			return attempt, nil
		}
		if attemptErr != nil || rt.isFailoverStatus(status, req.group.FailoverOn4xx) {
			if attemptErr == nil {
				attemptErr = fmt.Errorf("upstream status %d", status)
				attempt.streamMu.Lock()
				attempt.Err = attemptErr
				attempt.streamMu.Unlock()
			}
			return attempt, attemptErr
		}
		return attempt, nil
	case result := <-streamDone:
		attempt.setStreamResult(result)
		attempt.applyStreamResult(result)
		attempt.streamMu.Lock()
		validation := attempt.Validation
		status := attempt.Status
		attemptErr := attempt.Err
		if status >= 400 {
			attempt.Terminal = true
		}
		attempt.streamMu.Unlock()
		if validation.IsRejected() {
			return attempt, nil
		}
		if attemptErr != nil || rt.isFailoverStatus(status, req.group.FailoverOn4xx) {
			if attemptErr == nil {
				attemptErr = fmt.Errorf("upstream status %d", status)
				attempt.streamMu.Lock()
				attempt.Err = attemptErr
				attempt.streamMu.Unlock()
			}
			return attempt, attemptErr
		}
		return attempt, nil
	case <-ctx.Done():
		gate.Lose()
		cancel()
		attempt.streamMu.Lock()
		attemptErr := ctx.Err()
		attempt.Err = attemptErr
		attempt.streamMu.Unlock()
		return attempt, attemptErr
	}
}

func (a *coordinatedForwardAttempt) applyStreamResult(result streamAttemptResult) {
	a.streamMu.Lock()
	defer a.streamMu.Unlock()
	a.applyStreamResultLocked(result)
}

func (a *coordinatedForwardAttempt) applyStreamResultLocked(result streamAttemptResult) {
	a.Status = result.Status
	a.Headers = result.Headers
	a.UpstreamBody = result.Body
	a.Tokens = result.Tokens
	a.FirstTokenMS = result.FirstTokenMS
	a.Err = result.Err
	if result.ValidationRejection.IsRejected() {
		a.Validation = result.ValidationRejection
	}
	if result.PostCommitValidation.IsRejected() {
		a.Validation = result.PostCommitValidation
	}
	a.DurationMS = time.Since(a.StartedAt).Milliseconds()
}

func (a *coordinatedForwardAttempt) validationSnapshot() (validationResult, int, error, bool) {
	a.streamMu.Lock()
	defer a.streamMu.Unlock()
	return a.Validation, a.Status, a.Err, a.Terminal
}

func (a *coordinatedForwardAttempt) retryFailureSnapshot() (validationResult, int, error, bool, usageErrorInfo) {
	a.streamMu.Lock()
	defer a.streamMu.Unlock()
	return a.Validation, a.Status, a.Err, a.Terminal, a.ErrInfo
}

func coordinatedAttemptSuppressesSameRouteRetries(attempt *coordinatedForwardAttempt) bool {
	if attempt == nil {
		return false
	}
	validation, status, attemptErr, terminal, errInfo := attempt.retryFailureSnapshot()
	if validation.IsRejected() {
		return false
	}
	failed := attemptErr != nil || terminal || status < 200 || status >= 300
	return failed && !isSameRouteRetryableUpstreamFailure(status, errInfo)
}

func (rt *Runtime) cleanupCoordinatedStreamLosers(result hedgeRunResult[*coordinatedForwardAttempt], states *sync.Map) {
	winnerNumber := 0
	if result.Winner != nil {
		winnerNumber = result.Winner.Info.Number
	}
	states.Range(func(key, value any) bool {
		number, _ := key.(int)
		attempt, _ := value.(*coordinatedForwardAttempt)
		if attempt == nil || number == winnerNumber {
			return true
		}
		_, _, streamDone, ready := attempt.streamControlSnapshot()
		attempt.cancelStreamLoser()
		if ready {
			return true
		}
		if streamDone == nil {
			// The runner may still be resolving its route. Its shared context is
			// already canceled; auditCoordinatedAttempts will synthesize a
			// cancellation row if it misses the cleanup deadline.
			return true
		}
		select {
		case streamResult := <-streamDone:
			attempt.setStreamResult(streamResult)
			attempt.applyStreamResult(streamResult)
		default:
			// runHedge already gave canceled losers a bounded grace period. Do
			// not add a second blocking wait after the winner stream has ended;
			// auditCoordinatedAttempts safely emits a synthetic cancellation row
			// for a runner that is still unwinding.
		}
		return true
	})
}

func (rt *Runtime) auditCoordinatedAttempts(req *coordinatedForwardRequest, plan []coordinatedRoutePlan, result hedgeRunResult[*coordinatedForwardAttempt], states *sync.Map) uint {
	winnerNumber := 0
	if result.Winner != nil {
		winnerNumber = result.Winner.Info.Number
	}
	attempts := make(map[int]*coordinatedForwardAttempt, len(result.Attempts))
	outcomes := make(map[int]hedgeAttemptOutcome, len(result.Attempts))
	rejections := make(map[int]error, len(result.Attempts))
	infos := make(map[int]hedgeAttemptInfo, len(result.Attempts))
	for _, item := range result.Attempts {
		infos[item.Info.Number] = item.Info
		outcomes[item.Info.Number] = item.Outcome
		rejections[item.Info.Number] = item.Rejection
		if item.Value != nil {
			attempts[item.Info.Number] = item.Value
			if states != nil {
				states.Store(item.Info.Number, item.Value)
			}
		}
	}
	// Keep a state entry even when a cancellation-resistant runner did not
	// report a completion before the coordinator's cleanup deadline. Such an
	// attempt still incurred an upstream-side attempt and must be auditable.
	if states != nil {
		states.Range(func(key, value any) bool {
			number, ok := key.(int)
			if !ok || number <= 0 {
				return true
			}
			if attempt, ok := value.(*coordinatedForwardAttempt); ok && attempt != nil {
				if _, exists := attempts[number]; !exists {
					attempts[number] = attempt
				}
			}
			if _, exists := outcomes[number]; !exists {
				outcomes[number] = hedgeOutcomeCanceled
			}
			return true
		})
	}
	numbers := make([]int, 0, len(outcomes))
	for number := range outcomes {
		numbers = append(numbers, number)
	}
	for number := range attempts {
		if _, exists := outcomes[number]; !exists {
			outcomes[number] = hedgeOutcomeCanceled
			numbers = append(numbers, number)
		}
	}
	sort.Ints(numbers)

	var winnerUsageID uint
	for _, number := range numbers {
		attempt := attempts[number]
		if attempt == nil {
			entry := planEntryForAttempt(plan, number)
			attempt = &coordinatedForwardAttempt{
				Info: infos[number], Plan: entry,
				Route: entry.Candidate.Route, Err: context.Canceled,
				ErrInfo: usageErrorInfo{Type: "canceled", Summary: "attempt cleanup timed out after cancellation"},
			}
			if number == 1 && req.affinity.Recovery && attempt.Route.ID == req.affinity.RecoveryRouteID {
				attempt.Recovery = true
			}
		} else if attempt.runnerDone != nil {
			select {
			case <-attempt.runnerDone:
			default:
				// Do not read a live attempt's unsynchronized fields. The synthetic
				// cancellation record preserves the attempt count without racing the
				// runner that is still unwinding.
				entry := planEntryForAttempt(plan, number)
				liveAttempt := attempt
				info := infos[number]
				if info.Number == 0 {
					info = liveAttempt.Info
				}
				attempt = &coordinatedForwardAttempt{
					Info: info, Plan: entry,
					Route: entry.Candidate.Route, Err: context.Canceled,
					ErrInfo: usageErrorInfo{Type: "canceled", Summary: "attempt cleanup timed out after cancellation"},
				}
				attempt.upstreamStarted.Store(liveAttempt.didStartUpstream())
				if number == 1 && req.affinity.Recovery && attempt.Route.ID == req.affinity.RecoveryRouteID {
					attempt.Recovery = true
				}
			}
		}
		if attempt == nil || attempt.Skipped {
			continue
		}
		outcome := outcomes[number]
		rejection := rejections[number]
		isWinner := number == winnerNumber
		attemptKind := coordinatedAttemptKind(attempt, req.hedgeActive, number)
		attemptStatus := storage.GatewayAttemptStatusError
		var (
			status       int
			headers      http.Header
			upstreamBody []byte
			tokens       UsageTokens
			firstTokenMS *int64
			durationMS   int64
			attemptErr   error
			errInfo      usageErrorInfo
			validation   validationResult
			streamResult streamAttemptResult
			streamReady  bool
		)
		if req.stream {
			attempt.streamMu.Lock()
			status = attempt.Status
			headers = attempt.Headers
			upstreamBody = attempt.UpstreamBody
			tokens = attempt.Tokens
			firstTokenMS = attempt.FirstTokenMS
			durationMS = attempt.DurationMS
			attemptErr = attempt.Err
			errInfo = attempt.ErrInfo
			validation = attempt.Validation
			streamResult = attempt.streamResult
			streamReady = attempt.streamReady
			attempt.streamMu.Unlock()
		} else {
			status = attempt.Status
			headers = attempt.Headers
			upstreamBody = attempt.UpstreamBody
			tokens = attempt.Tokens
			firstTokenMS = attempt.FirstTokenMS
			durationMS = attempt.DurationMS
			attemptErr = attempt.Err
			errInfo = attempt.ErrInfo
			validation = attempt.Validation
		}
		if validation.IsRejected() && !validation.PostCommit {
			attemptKind = storage.GatewayAttemptKindRegexReject
			attemptStatus = storage.GatewayAttemptStatusRejected
			errInfo = validationErrorInfo(validation)
		} else if outcome == hedgeOutcomeRejected && errors.Is(rejection, errSkippedRejectedRoute) {
			attemptStatus = storage.GatewayAttemptStatusCanceled
			if strings.TrimSpace(errInfo.Summary) == "" {
				errInfo = usageErrorInfo{Type: "canceled", Summary: "attempt canceled after response validation rejected its route"}
			}
		} else if outcome == hedgeOutcomeCanceled || outcome == hedgeOutcomeLost || errors.Is(attemptErr, context.Canceled) {
			attemptStatus = storage.GatewayAttemptStatusCanceled
			if strings.TrimSpace(errInfo.Summary) == "" {
				errInfo = usageErrorInfo{Type: "canceled", Summary: "attempt canceled after another upstream won"}
			}
		} else if isWinner && status >= 200 && status < 300 {
			attemptStatus = storage.GatewayAttemptStatusAccepted
		} else if status >= 200 && status < 300 && attemptErr == nil {
			attemptStatus = storage.GatewayAttemptStatusAccepted
		}

		if strings.TrimSpace(errInfo.Summary) == "" && attemptErr != nil {
			errInfo = rt.buildUpstreamErrorInfoCfg(
				rt.gatewayRuntime(), attemptErr, status, headers,
				upstreamBody, attempt.UpstreamURL, req.c.Request.Method,
			)
		}
		if strings.TrimSpace(errInfo.Summary) == "" && status >= 400 {
			errInfo = rt.buildUpstreamErrorInfoCfg(
				rt.gatewayRuntime(), nil, status, headers,
				upstreamBody, attempt.UpstreamURL, req.c.Request.Method,
			)
		}
		if isWinner && validation.IsRejected() && validation.PostCommit {
			attemptStatus = storage.GatewayAttemptStatusAccepted
		}
		suppressSameRouteRetry := !validation.IsRejected() && !isSameRouteRetryableUpstreamFailure(status, errInfo)
		if attemptStatus == storage.GatewayAttemptStatusError && req.group.RetryEnabled &&
			req.group.CooldownSeconds > 0 &&
			(suppressSameRouteRetry || !coordinatedPlanHasLaterRoute(plan, number, attempt.Route.ID)) {
			until := time.Now().Add(time.Duration(req.group.CooldownSeconds) * time.Second)
			pauseReason := errInfo.Summary
			if strings.TrimSpace(errInfo.Detail) != "" {
				pauseReason = rt.truncateRunes(errInfo.Detail, 4000)
			}
			cooldownErr := rt.Routes.SetModelTempUnschedulable(
				attempt.Route.ID, attempt.UpstreamModel, until, pauseReason, time.Now(), req.requestID,
			)
			if cooldownErr == nil && storage.NormalizeGatewayModel(attempt.UpstreamModel) != "" {
				req.affinity.preservePreferredOnCooldown(attempt.Route.ID)
			}
			attempt.UsageMeta.CooldownUntil = &until
		}
		success := status >= 200 && status < 300 && attemptErr == nil &&
			(!validation.IsRejected() || validation.PostCommit)
		if req.stream && streamReady {
			onlyClientDisconnect := rt.isClientDisconnectAfterCommit(streamResult.ClientDisconnected, streamResult.StreamErr)
			success = success && (streamResult.StreamErr == nil || onlyClientDisconnect)
		}
		if attempt.Recovery {
			if success {
				if !isWinner {
					rt.noteRouteModelSuccess(&attempt.Route, attempt.UpstreamModel)
				}
				rt.finishRouteAffinityProbe(&req.affinity, attempt.Route.ID, true, nil, time.Now())
				if req.affinity.shouldRememberRoute(attempt.Route.ID) {
					rt.rememberRouteAffinity(req.affinity.Keys, attempt.Route.ID, time.Now())
				}
			} else {
				blockUntil := req.affinity.recoveryBlockedUntil(attempt.UsageMeta.CooldownUntil, time.Now())
				rt.finishRouteAffinityProbe(&req.affinity, attempt.Route.ID, false, blockUntil, time.Now())
			}
		}

		meta := attempt.UsageMeta
		meta.Attempt = number
		meta.AttemptKind = attemptKind
		meta.AttemptStatus = attemptStatus
		// Winner settlement is deliberately deferred until the downstream write
		// succeeds. The finalizer is the only place that may mark a usage row as
		// winner or charge the gateway key.
		meta.Winner = false
		meta.DeferSettlement = true
		meta.Validation = validation
		usageID := rt.recordUsage(
			req.key, req.group, &attempt.Route, attempt.Target,
			req.requestID, req.requestedModel, attempt.UpstreamModel, attempt.MappingChain,
			tokens, attempt.Plan.Candidate.EffectiveRate, attempt.Plan.Candidate.BillingRate,
			req.stream, status, success, errInfo, durationMS,
			firstTokenMS, req.c, meta,
		)
		if isWinner {
			winnerUsageID = usageID
		}
	}
	return winnerUsageID
}

func planEntryForAttempt(plan []coordinatedRoutePlan, number int) coordinatedRoutePlan {
	if number <= 0 || number > len(plan) {
		return coordinatedRoutePlan{}
	}
	return plan[number-1]
}

func coordinatedPlanHasLaterRoute(plan []coordinatedRoutePlan, number int, routeID uint) bool {
	for index := number; index < len(plan); index++ {
		if plan[index].Candidate.Route.ID == routeID {
			return true
		}
	}
	return false
}

func removeRecoveryRouteRetries(plan []coordinatedRoutePlan, routeID uint) []coordinatedRoutePlan {
	if routeID == 0 || len(plan) == 0 {
		return plan
	}
	out := plan[:0]
	for _, entry := range plan {
		if entry.Candidate.Route.ID == routeID && entry.TryOnRoute > 0 {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func coordinatedAttemptKind(attempt *coordinatedForwardAttempt, hedgeActive bool, number int) string {
	if attempt != nil && attempt.Recovery && number == 1 {
		return storage.GatewayAttemptKindRecovery
	}
	if number <= 1 {
		return storage.GatewayAttemptKindPrimary
	}
	// runHedge marks auxiliary attempts as hedge only in its concurrent
	// scheduler. When MaxParallel is one it falls back to the sequential
	// retry ladder, whose attempts must remain retry/failover for accounting.
	if attempt != nil && attempt.Info.Kind == attemptKindHedge && attempt.Info.Concurrent && attempt.didStartUpstream() {
		return storage.GatewayAttemptKindHedge
	}
	if attempt != nil && attempt.Plan.TryOnRoute > 0 {
		return storage.GatewayAttemptKindRetry
	}
	return storage.GatewayAttemptKindFailover
}

// coordinatedHedgeTriggered reports whether at least one auxiliary attempt
// overlapped another in-flight attempt. The result may contain a primary
// winner plus a canceled hedge, which still incurred upstream work and
// therefore qualifies for the virtual cache settlement. A fast failure or
// validation rejection that starts the next attempt after the first one has
// already completed remains sequential and is excluded.
func coordinatedHedgeTriggered(result hedgeRunResult[*coordinatedForwardAttempt], states *sync.Map) bool {
	for _, attempt := range result.Attempts {
		if attempt.Info.Kind != attemptKindHedge || !attempt.Info.Concurrent {
			continue
		}
		value := attempt.Value
		if value == nil && states != nil {
			if stored, ok := states.Load(attempt.Info.Number); ok {
				value, _ = stored.(*coordinatedForwardAttempt)
			}
		}
		if value != nil && value.didStartUpstream() {
			return true
		}
	}
	return false
}

func (rt *Runtime) virtualCacheReasonForWinner(req *coordinatedForwardRequest, winner *coordinatedForwardAttempt, states *sync.Map) string {
	if req == nil || req.group == nil || winner == nil || !winner.didStartUpstream() {
		return ""
	}
	var header http.Header
	if req.c != nil && req.c.Request != nil {
		header = req.c.Request.Header
	}
	modelForEligibility := strings.TrimSpace(winner.UpstreamModel)
	if modelForEligibility == "" {
		modelForEligibility = req.requestedModel
	}
	eligible := req.virtualCacheEligible
	if !req.hedgeEligibilityKnown {
		eligible = hedgeEligible(hedgeRequest{
			Path: req.path, Model: modelForEligibility, Header: header,
			Body: req.body, Stream: req.stream, Realtime: strings.Contains(strings.ToLower(req.path), "realtime"),
		})
	}
	if !eligible || mediaGenerationModel(modelForEligibility) {
		return ""
	}
	if req.group.HedgeVirtualCacheEnabled && states != nil {
		realConcurrentAttempt := false
		states.Range(func(_, value any) bool {
			attempt, _ := value.(*coordinatedForwardAttempt)
			if attempt != nil && attempt.Info.Kind == attemptKindHedge && attempt.Info.Concurrent && attempt.didStartUpstream() {
				realConcurrentAttempt = true
				return false
			}
			return true
		})
		if realConcurrentAttempt {
			return storage.GatewayVirtualCacheReasonHedge
		}
	}
	if req.group.ResponseValidationVirtualCacheEnabled && states != nil {
		winnerKind := coordinatedAttemptKind(winner, req.hedgeActive, winner.Info.Number)
		if winnerKind == storage.GatewayAttemptKindFailover || winnerKind == storage.GatewayAttemptKindHedge {
			priorRejectedRoute := false
			states.Range(func(_, value any) bool {
				attempt, _ := value.(*coordinatedForwardAttempt)
				if attempt == nil || attempt.Info.Number >= winner.Info.Number || !attempt.didStartUpstream() || attempt.Route.ID == winner.Route.ID {
					return true
				}
				validation, _, _, _ := attempt.validationSnapshot()
				if validation.IsRejected() && !validation.PostCommit {
					priorRejectedRoute = true
					return false
				}
				return true
			})
			if priorRejectedRoute {
				return storage.GatewayVirtualCacheReasonResponseRuleFailover
			}
		}
	}
	if winner.Target != nil {
		if percent, err := ProviderVirtualCachePercentForModel(winner.Target.Provider, modelForEligibility); err == nil && percent > 0 {
			return storage.GatewayVirtualCacheReasonProviderGlobal
		}
	}
	return ""
}

func (rt *Runtime) finishCoordinatedNonStream(req *coordinatedForwardRequest, winner *coordinatedForwardAttempt, usageID uint) {
	if winner == nil {
		rt.finalizeUsageFailure(req.requestID, req.key)
		return
	}
	settlement := rt.buildVirtualCacheSettlement(req, winner)
	if settlement.VirtualCacheReadEnabled {
		percent := 100
		if settlement.VirtualCacheReason == storage.GatewayVirtualCacheReasonProviderGlobal && winner.Target != nil {
			percent, _ = ProviderVirtualCachePercentForModel(winner.Target.Provider, winner.UpstreamModel)
		}
		if rewritten, changed := rewriteVirtualCacheResponsePercent(winner.ClientBody, req.kind, percent); changed {
			winner.ClientBody = rewritten
		} else {
			settlement = storage.GatewayFinalizeRequestInput{}
		}
	}
	rt.copyResponseHeaders(req.c.Writer.Header(), winner.Headers)
	if winner.Converted || winner.Status >= 400 {
		req.c.Writer.Header().Del("Content-Length")
		req.c.Header("Content-Type", "application/json")
	}
	if settlement.VirtualCacheReadEnabled {
		req.c.Writer.Header().Del("Content-Length")
	}
	rt.setGatewayRequestIDHeaders(req.c, req.requestID)
	status := winner.Status
	if status == 0 {
		status = http.StatusBadGateway
	}
	req.c.Status(status)
	_, writeErr := req.c.Writer.Write(winner.ClientBody)
	if writeErr != nil || status < 200 || status >= 300 || usageID == 0 {
		rt.finalizeUsageFailure(req.requestID, req.key)
		return
	}
	rt.noteRouteModelSuccess(&winner.Route, winner.UpstreamModel)
	rt.finishRouteAffinityProbe(&req.affinity, winner.Route.ID, true, nil, time.Now())
	if req.affinity.shouldRememberRoute(winner.Route.ID) {
		rt.rememberRouteAffinity(req.affinity.Keys, winner.Route.ID, time.Now())
	}
	if err := rt.finalizeUsageWinnerWithSettlement(req.requestID, req.key, winner.Info.Number, usageID, settlement); err != nil {
		if rt.Log != nil {
			rt.Log.Error("finalize coordinated gateway winner failed", "request_id", req.requestID, "attempt", winner.Info.Number, "err", err)
		}
		// A virtual-cache validation failure must never leave a delivered request
		// uncharged. Retry the idempotent finalizer without the optional credit.
		if settlement.VirtualCacheReadEnabled {
			if fallbackErr := rt.finalizeUsageWinner(req.requestID, req.key, winner.Info.Number, usageID); fallbackErr != nil && rt.Log != nil {
				rt.Log.Error("fallback coordinated gateway winner finalization failed", "request_id", req.requestID, "attempt", winner.Info.Number, "err", fallbackErr)
			}
		}
	}
}

func (rt *Runtime) finishCoordinatedStream(req *coordinatedForwardRequest, winner *coordinatedForwardAttempt, usageID uint) {
	if winner == nil {
		rt.finalizeUsageFailure(req.requestID, req.key)
		return
	}
	gate, _, _, _ := winner.streamControlSnapshot()
	if gate == nil {
		rt.finalizeUsageFailure(req.requestID, req.key)
		return
	}
	streamResult := winner.awaitStreamResult()
	onlyClientDisconnect := rt.isClientDisconnectAfterCommit(streamResult.ClientDisconnected, streamResult.StreamErr)
	success := winner.gateCommitError() == nil && gate.CommitError() == nil && gate.DownstreamCommitted() &&
		winner.Status >= 200 && winner.Status < 300 &&
		(streamResult.StreamErr == nil || onlyClientDisconnect)
	if !success || usageID == 0 {
		rt.finalizeUsageFailure(req.requestID, req.key)
		return
	}
	rt.noteRouteModelSuccess(&winner.Route, winner.UpstreamModel)
	rt.finishRouteAffinityProbe(&req.affinity, winner.Route.ID, true, nil, time.Now())
	if req.affinity.shouldRememberRoute(winner.Route.ID) {
		rt.rememberRouteAffinity(req.affinity.Keys, winner.Route.ID, time.Now())
	}
	settlement := rt.buildVirtualCacheSettlement(req, winner)
	if settlement.VirtualCacheReadEnabled && !gate.VirtualCacheApplied() {
		settlement = storage.GatewayFinalizeRequestInput{}
	}
	if err := rt.finalizeUsageWinnerWithSettlement(req.requestID, req.key, winner.Info.Number, usageID, settlement); err != nil {
		if rt.Log != nil {
			rt.Log.Error("finalize coordinated stream winner failed", "request_id", req.requestID, "attempt", winner.Info.Number, "err", err)
		}
		if settlement.VirtualCacheReadEnabled {
			if fallbackErr := rt.finalizeUsageWinner(req.requestID, req.key, winner.Info.Number, usageID); fallbackErr != nil && rt.Log != nil {
				rt.Log.Error("fallback coordinated stream winner finalization failed", "request_id", req.requestID, "attempt", winner.Info.Number, "err", fallbackErr)
			}
		}
	}
}

func (rt *Runtime) writeCoordinatedFailure(req *coordinatedForwardRequest, result hedgeRunResult[*coordinatedForwardAttempt], runErr error) {
	var (
		found          bool
		lastStatus     int
		lastBody       []byte
		lastHeaders    http.Header
		lastErr        error
		lastValidation validationResult
	)
	for i := len(result.Attempts) - 1; i >= 0; i-- {
		attempt := result.Attempts[i].Value
		if attempt == nil || attempt.Skipped {
			continue
		}
		found = true
		if req.stream {
			attempt.streamMu.Lock()
			lastStatus = attempt.Status
			lastBody = append([]byte(nil), attempt.ClientBody...)
			lastHeaders = attempt.Headers.Clone()
			lastErr = attempt.Err
			lastValidation = attempt.Validation
			attempt.streamMu.Unlock()
		} else {
			lastStatus = attempt.Status
			lastBody = attempt.ClientBody
			lastHeaders = attempt.Headers
			lastErr = attempt.Err
			lastValidation = attempt.Validation
		}
		if lastStatus > 0 || lastErr != nil {
			break
		}
	}
	// A response rejected by a validation rule must never be replayed merely
	// because the attempt chain is exhausted. Doing so would bypass the rule.
	if found && !lastValidation.IsRejected() && lastStatus > 0 && len(lastBody) > 0 {
		body := rt.injectUpstreamOpsRequestID(lastBody, req.requestID)
		rt.copyResponseHeaders(req.c.Writer.Header(), lastHeaders)
		rt.setGatewayRequestIDHeaders(req.c, req.requestID)
		req.c.Writer.Header().Del("Content-Length")
		req.c.Header("Content-Type", "application/json")
		req.c.Status(lastStatus)
		_, _ = req.c.Writer.Write(body)
		return
	}
	message := "all upstream routes failed"
	if found && lastValidation.IsRejected() {
		message = validationErrorInfo(lastValidation).Summary
	} else if found && lastErr != nil && !errors.Is(lastErr, errSkippedRejectedRoute) {
		message = lastErr.Error()
	} else if runErr != nil && !errors.Is(runErr, errHedgeExhausted) {
		message = runErr.Error()
	}
	rt.writeGatewayError(req.c, req.kind, http.StatusBadGateway, "api_error", message)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
