package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

const (
	defaultHedgeDelay       = 10 * time.Second
	defaultHedgeMaxParallel = 2
	defaultHedgeMaxAttempts = 4
	maxHedgeAttempts        = 64
)

var (
	hedgeCleanupTimeout       = 5 * time.Second
	hedgeWinnerCleanupTimeout = 100 * time.Millisecond
)

var (
	errHedgeExhausted = errors.New("hedge attempts exhausted without an accepted response")
	errHedgeCanceled  = errors.New("hedge request canceled")
)

// hedgeTerminalError stops the attempt ladder without selecting a winner.
// It is used when an auxiliary feature (for example response validation) has
// a larger route plan than the legacy transport failover policy permits.
type hedgeTerminalError struct{ err error }

func (e *hedgeTerminalError) Error() string {
	if e == nil || e.err == nil {
		return "hedge attempt terminated"
	}
	return e.err.Error()
}

func (e *hedgeTerminalError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func stopHedgeAttempts(err error) error {
	if err == nil {
		err = errHedgeExhausted
	}
	return &hedgeTerminalError{err: err}
}

// hedgePolicy controls the launch ladder. MaxParallel includes the primary
// request and MaxAttempts is the total request budget.
type hedgePolicy struct {
	Enabled     bool
	Delay       time.Duration
	MaxParallel int
	MaxAttempts int
}

func (p hedgePolicy) normalized() hedgePolicy {
	if p.Delay <= 0 {
		p.Delay = defaultHedgeDelay
	}
	if p.MaxParallel <= 0 {
		p.MaxParallel = defaultHedgeMaxParallel
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = defaultHedgeMaxAttempts
	}
	if p.MaxAttempts > maxHedgeAttempts {
		p.MaxAttempts = maxHedgeAttempts
	}
	if p.MaxParallel > p.MaxAttempts {
		p.MaxParallel = p.MaxAttempts
	}
	if p.MaxParallel > maxHedgeAttempts {
		p.MaxParallel = maxHedgeAttempts
	}
	return p
}

type hedgeRequest struct {
	Path                    string
	Model                   string
	Header                  http.Header
	Body                    []byte
	Stream                  bool
	Realtime                bool
	BodyMediaAnalyzed       bool
	BodyGeneratesMedia      bool
}

// hedgeEligible excludes operations that can create image/video assets and
// Realtime/WebSocket requests. Multimodal text requests with image inputs stay
// eligible because they do not generate media assets.
func hedgeEligible(req hedgeRequest) bool {
	if req.Realtime || isWebSocketRequest(req.Header) {
		return false
	}
	path := strings.ToLower(strings.TrimSpace(req.Path))
	for _, marker := range []string{
		"/images/generations", "/images/edits", "/images/batches",
		"/videos/generations", "/videos/edits", "/videos/extensions", "/realtime",
	} {
		if strings.Contains(path, marker) {
			return false
		}
	}
	if mediaGenerationModel(req.Model) {
		return false
	}
	if req.BodyMediaAnalyzed {
		return !req.BodyGeneratesMedia
	}
	if len(req.Body) == 0 {
		return true
	}
	var payload any
	if err := json.Unmarshal(req.Body, &payload); err != nil {
		return true
	}
	return !mediaGenerationPayload(payload)
}

func isWebSocketRequest(header http.Header) bool {
	if header == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(header.Get("Upgrade")), "websocket") ||
		strings.TrimSpace(header.Get("Sec-WebSocket-Key")) != ""
}

func mediaGenerationPayload(payload any) bool {
	object, ok := payload.(map[string]any)
	if !ok {
		return false
	}
	if model, _ := object["model"].(string); mediaGenerationModel(model) {
		return true
	}
	for _, key := range []string{"responseModalities", "response_modalities", "outputModalities", "output_modalities", "modalities"} {
		if hasGeneratedMediaModality(object[key]) {
			return true
		}
	}
	for _, key := range []string{"responseMimeType", "response_mime_type", "outputMimeType", "output_mime_type"} {
		if value, ok := object[key].(string); ok && mediaMIMEType(value) {
			return true
		}
	}
	for _, key := range []string{"image_generation", "imageGeneration", "video_generation", "videoGeneration"} {
		if value, exists := object[key]; exists {
			switch typed := value.(type) {
			case nil:
			case bool:
				if typed {
					return true
				}
			default:
				return true
			}
		}
	}
	for _, key := range []string{"task", "operation"} {
		if value, ok := object[key].(string); ok && mediaGenerationType(value) {
			return true
		}
	}
	if tools, ok := object["tools"].([]any); ok {
		for _, item := range tools {
			tool, _ := item.(map[string]any)
			if value, ok := tool["type"].(string); ok && mediaGenerationType(value) {
				return true
			}
		}
	}
	return false
}

func hasGeneratedMediaModality(value any) bool {
	items, ok := value.([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		text, _ := item.(string)
		switch strings.ToLower(strings.TrimSpace(text)) {
		case "image", "video":
			return true
		}
	}
	return false
}

func mediaMIMEType(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return strings.HasPrefix(value, "image/") || strings.HasPrefix(value, "video/")
}

func mediaGenerationType(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.NewReplacer("_", "", "-", "", ".", "").Replace(value)
	for _, marker := range []string{"imagegeneration", "videogeneration", "texttoimage", "texttovideo", "generateimage", "generatevideo"} {
		if value == marker {
			return true
		}
	}
	return false
}

func mediaGenerationModel(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, marker := range []string{
		"dall-e", "dalle", "gpt-image", "image-generation", "image_generation", "imagen",
		"flash-image", "pro-image", "flux", "stable-diffusion", "stable_diffusion", "sdxl",
		"ideogram", "recraft", "seedream", "veo", "sora", "video-generation",
		"video_generation", "kling", "wan2", "wan-", "cogvideo", "luma", "runway", "pika",
	} {
		if strings.Contains(value, marker) {
			return true
		}
	}
	return false
}

func mappedRouteContainsMediaModel(routes []storage.GatewayRoute, requested string, groupMapping ModelMap) bool {
	if mediaGenerationModel(requested) {
		return true
	}
	for _, route := range routes {
		upstream, _ := ResolveModel(requested, ParseModelMapping(route.ModelMappingJSON), groupMapping)
		if mediaGenerationModel(upstream) {
			return true
		}
	}
	return false
}

type hedgeAttemptInfo struct {
	Number    int
	Kind      string
	StartedAt time.Time
	// Concurrent is true only when this auxiliary attempt was launched while
	// another attempt was still active. A sequential retry can still use the
	// hedge scheduler's attempt kind, but it must not receive hedge-only credit.
	Concurrent bool
}

type hedgeAttemptOutcome string

const (
	hedgeOutcomeAccepted hedgeAttemptOutcome = "accepted"
	hedgeOutcomeError    hedgeAttemptOutcome = "error"
	hedgeOutcomeRejected hedgeAttemptOutcome = "rejected"
	hedgeOutcomeCanceled hedgeAttemptOutcome = "canceled"
	hedgeOutcomeLost     hedgeAttemptOutcome = "lost"
)

type hedgeAttemptResult[T any] struct {
	Info       hedgeAttemptInfo
	Value      T
	Err        error
	Rejection  error
	Outcome    hedgeAttemptOutcome
	Accepted   bool
	FinishedAt time.Time
}

type hedgeRunResult[T any] struct {
	Value      T
	Winner     *hedgeAttemptResult[T]
	Attempts   []hedgeAttemptResult[T]
	StartedAt  time.Time
	FinishedAt time.Time
}

type hedgeAttemptFunc[T any] func(context.Context, hedgeAttemptInfo) (T, error)
type hedgeValidateFunc[T any] func(T) (bool, error)

type hedgeHooks[T any] struct {
	OnStart    func(hedgeAttemptInfo)
	OnComplete func(hedgeAttemptResult[T])
	OnWinner   func(hedgeAttemptResult[T])
	OnCanceled func(hedgeAttemptInfo)
}

type hedgeCompletion[T any] struct {
	result hedgeAttemptResult[T]
}

// runHedge starts the primary immediately, schedules subsequent attempts from
// the primary start time, and refills a slot immediately after a hard failure
// or validation rejection. The first accepted result wins and cancels losers.
func runHedge[T any](ctx context.Context, eligible bool, policy hedgePolicy, run hedgeAttemptFunc[T], validate hedgeValidateFunc[T], hooks hedgeHooks[T]) (hedgeRunResult[T], error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if run == nil {
		return hedgeRunResult[T]{}, errors.New("hedge attempt function is nil")
	}
	if !policy.Enabled || !eligible {
		maxAttempts := policy.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = defaultHedgeMaxAttempts
		}
		return runSequentialHedge(ctx, maxAttempts, run, validate, hooks)
	}
	policy = policy.normalized()
	if policy.MaxParallel <= 1 || policy.MaxAttempts <= 1 {
		return runSequentialHedge(ctx, policy.MaxAttempts, run, validate, hooks)
	}

	startedAt := time.Now()
	result := hedgeRunResult[T]{StartedAt: startedAt}
	coordCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	completed := make(map[int]hedgeAttemptResult[T], policy.MaxAttempts)
	active := make(map[int]hedgeAttemptInfo, policy.MaxParallel)
	completions := make(chan hedgeCompletion[T], policy.MaxAttempts)
	started := 0
	nextDue := startedAt

	launch := func() {
		started++
		info := hedgeAttemptInfo{
			Number: started, Kind: attemptKindHedge, StartedAt: time.Now(),
			Concurrent: len(active) > 0,
		}
		if started == 1 {
			info.Kind = attemptKindPrimary
		}
		active[info.Number] = info
		if hooks.OnStart != nil {
			hooks.OnStart(info)
		}
		go func() {
			value, err := run(coordCtx, info)
			completions <- hedgeCompletion[T]{result: hedgeAttemptResult[T]{
				Info: info, Value: value, Err: err, FinishedAt: time.Now(),
			}}
		}()
		nextDue = startedAt.Add(time.Duration(started) * policy.Delay)
	}

	for {
		if err := ctx.Err(); err != nil {
			cancel()
			collectActiveHedgeAttempts(active, completions, completed, validate, hooks, false, hedgeCleanupTimeout)
			result.Attempts = orderedHedgeAttempts(completed)
			result.FinishedAt = time.Now()
			return result, errors.Join(errHedgeCanceled, err)
		}
		if started < policy.MaxAttempts && len(active) < policy.MaxParallel && !time.Now().Before(nextDue) {
			launch()
			continue
		}
		if len(active) == 0 && started >= policy.MaxAttempts {
			result.Attempts = orderedHedgeAttempts(completed)
			result.FinishedAt = time.Now()
			return result, errHedgeExhausted
		}

		var timer *time.Timer
		var timerC <-chan time.Time
		if started < policy.MaxAttempts && len(active) < policy.MaxParallel {
			wait := time.Until(nextDue)
			if wait < 0 {
				wait = 0
			}
			timer = time.NewTimer(wait)
			timerC = timer.C
		}
		select {
		case <-ctx.Done():
			stopHedgeTimer(timer)
			continue
		case <-timerC:
			continue
		case completion := <-completions:
			stopHedgeTimer(timer)
			attempt := completion.result
			delete(active, attempt.Info.Number)
			var terminalErr *hedgeTerminalError
			terminal := errors.As(attempt.Err, &terminalErr)
			if attempt.Err != nil {
				attempt.Outcome = hedgeOutcomeError
			} else if validate == nil {
				attempt.Accepted = true
				attempt.Outcome = hedgeOutcomeAccepted
			} else {
				attempt.Accepted, attempt.Rejection = validate(attempt.Value)
				if attempt.Accepted {
					attempt.Outcome = hedgeOutcomeAccepted
				} else {
					attempt.Outcome = hedgeOutcomeRejected
				}
			}
			completed[attempt.Info.Number] = attempt
			if hooks.OnComplete != nil {
				hooks.OnComplete(attempt)
			}
			if terminal {
				cancel()
				collectActiveHedgeAttempts(active, completions, completed, validate, hooks, false, hedgeCleanupTimeout)
				result.Attempts = orderedHedgeAttempts(completed)
				result.FinishedAt = time.Now()
				return result, terminalErr.err
			}
			if attempt.Accepted {
				winner := attempt
				result.Value = attempt.Value
				result.Winner = &winner
				cancel()
				// Select the winner before waiting for canceled losers to drain.
				// Stream gates must release their buffered prefix immediately; a
				// slow or cancellation-resistant loser must not delay first bytes.
				if hooks.OnWinner != nil {
					hooks.OnWinner(attempt)
				}
				// The winner has already been exposed to downstream. Give canceled
				// losers only a short grace period to publish their final audit data;
				// a cancellation-resistant upstream must not hold the successful HTTP
				// response open for the full failure-cleanup timeout.
				collectActiveHedgeAttempts(active, completions, completed, validate, hooks, true, hedgeWinnerCleanupTimeout)
				result.Attempts = orderedHedgeAttempts(completed)
				result.FinishedAt = time.Now()
				return result, nil
			}
			// A completed rejection/failure frees capacity immediately. Other slow
			// attempts keep running and still count toward MaxParallel.
			if started < policy.MaxAttempts && len(active) < policy.MaxParallel {
				nextDue = time.Now()
			}
		}
	}
}

func runSequentialHedge[T any](ctx context.Context, maxAttempts int, run hedgeAttemptFunc[T], validate hedgeValidateFunc[T], hooks hedgeHooks[T]) (hedgeRunResult[T], error) {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	result := hedgeRunResult[T]{StartedAt: time.Now()}
	for number := 1; number <= maxAttempts; number++ {
		if err := ctx.Err(); err != nil {
			result.FinishedAt = time.Now()
			return result, errors.Join(errHedgeCanceled, err)
		}
		kind := attemptKindFailover
		if number == 1 {
			kind = attemptKindPrimary
		}
		info := hedgeAttemptInfo{Number: number, Kind: kind, StartedAt: time.Now()}
		if hooks.OnStart != nil {
			hooks.OnStart(info)
		}
		value, err := run(ctx, info)
		attempt := hedgeAttemptResult[T]{Info: info, Value: value, Err: err, FinishedAt: time.Now()}
		var terminalErr *hedgeTerminalError
		terminal := errors.As(err, &terminalErr)
		if err != nil {
			attempt.Outcome = hedgeOutcomeError
		} else if validate == nil {
			attempt.Accepted = true
			attempt.Outcome = hedgeOutcomeAccepted
		} else {
			attempt.Accepted, attempt.Rejection = validate(value)
			if attempt.Accepted {
				attempt.Outcome = hedgeOutcomeAccepted
			} else {
				attempt.Outcome = hedgeOutcomeRejected
			}
		}
		result.Attempts = append(result.Attempts, attempt)
		result.FinishedAt = attempt.FinishedAt
		if hooks.OnComplete != nil {
			hooks.OnComplete(attempt)
		}
		if terminal {
			return result, terminalErr.err
		}
		if attempt.Accepted {
			winner := attempt
			result.Winner = &winner
			result.Value = value
			if hooks.OnWinner != nil {
				hooks.OnWinner(attempt)
			}
			return result, nil
		}
	}
	return result, errHedgeExhausted
}

func collectActiveHedgeAttempts[T any](
	active map[int]hedgeAttemptInfo,
	completions <-chan hedgeCompletion[T],
	completed map[int]hedgeAttemptResult[T],
	validate hedgeValidateFunc[T],
	hooks hedgeHooks[T],
	winnerChosen bool,
	timeout time.Duration,
) {
	if len(active) == 0 {
		return
	}
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	timer := time.NewTimer(timeout)
	defer stopHedgeTimer(timer)
	for len(active) > 0 {
		select {
		case completion := <-completions:
			attempt := completion.result
			info, ok := active[attempt.Info.Number]
			if !ok {
				continue
			}
			delete(active, attempt.Info.Number)
			attempt.Info = info
			if attempt.Err != nil {
				if errors.Is(attempt.Err, context.Canceled) || errors.Is(attempt.Err, context.DeadlineExceeded) {
					attempt.Outcome = hedgeOutcomeCanceled
				} else {
					attempt.Outcome = hedgeOutcomeError
				}
			} else if validate == nil {
				attempt.Accepted = true
				attempt.Outcome = hedgeOutcomeLost
			} else {
				attempt.Accepted, attempt.Rejection = validate(attempt.Value)
				if attempt.Accepted {
					attempt.Accepted = false
					attempt.Outcome = hedgeOutcomeLost
				} else {
					attempt.Outcome = hedgeOutcomeRejected
				}
			}
			if !winnerChosen {
				attempt.Accepted = false
				attempt.Outcome = hedgeOutcomeCanceled
			}
			completed[attempt.Info.Number] = attempt
			if hooks.OnComplete != nil {
				hooks.OnComplete(attempt)
			}
			if hooks.OnCanceled != nil {
				hooks.OnCanceled(attempt.Info)
			}
		case <-timer.C:
			for number, info := range active {
				attempt := hedgeAttemptResult[T]{
					Info: info, Err: context.Canceled, Outcome: hedgeOutcomeCanceled, FinishedAt: time.Now(),
				}
				if winnerChosen {
					attempt.Outcome = hedgeOutcomeLost
				}
				completed[number] = attempt
				if hooks.OnCanceled != nil {
					hooks.OnCanceled(info)
				}
			}
			return
		}
	}
}

func orderedHedgeAttempts[T any](attempts map[int]hedgeAttemptResult[T]) []hedgeAttemptResult[T] {
	numbers := make([]int, 0, len(attempts))
	for number := range attempts {
		numbers = append(numbers, number)
	}
	sort.Ints(numbers)
	result := make([]hedgeAttemptResult[T], 0, len(numbers))
	for _, number := range numbers {
		result = append(result, attempts[number])
	}
	return result
}

func stopHedgeTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}
