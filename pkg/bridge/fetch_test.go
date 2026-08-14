package bridge

import (
	"encoding/base64"
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/VulpineOS/foxbridge/pkg/cdp"
)

func TestFetchEnable(t *testing.T) {
	b, mb := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.enable",
		Params: json.RawMessage(`{"patterns":[],"handleAuthRequests":true}`),
	}

	result, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}
	if string(result) != "{}" {
		t.Errorf("result = %s, want {}", string(result))
	}

	last, err := mb.LastCall()
	if err != nil {
		t.Fatal(err)
	}
	if last.Method != "Browser.setRequestInterception" {
		t.Errorf("method = %q, want Browser.setRequestInterception", last.Method)
	}

	var params map[string]interface{}
	json.Unmarshal(last.Params, &params)
	if params["enabled"] != true {
		t.Errorf("enabled = %v, want true", params["enabled"])
	}
}

func TestFetchEnableStoresPatterns(t *testing.T) {
	b, _ := newTestBridge()

	msg := &cdp.Message{
		ID:        1,
		Method:    "Fetch.enable",
		SessionID: "s1",
		Params: json.RawMessage(`{
			"patterns": [{
				"urlPattern": "http://hermes-dialog-bridge.invalid/*",
				"resourceType": "XHR",
				"requestStage": "Request"
			}]
		}`),
	}

	if _, cdpErr := b.handleFetch(nil, msg); cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}

	if !b.shouldPauseFetchRequest("s1", "http://hermes-dialog-bridge.invalid/?kind=alert", "XHR", "Request") {
		t.Error("matching dialog bridge request should be paused")
	}
	if b.shouldPauseFetchRequest("s1", "https://github.com/", "Document", "Request") {
		t.Error("non-matching navigation should not be paused")
	}
}

func TestFetchPatternsMatchCDPWildcards(t *testing.T) {
	b, _ := newTestBridge()
	patterns, err := compileFetchPatterns([]json.RawMessage{
		json.RawMessage(`{"urlPattern":"https://*.example.com/file?.js","requestStage":"Request"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	b.setFetchPatterns("s1", patterns)

	if !b.shouldPauseFetchRequest("s1", "https://cdn.example.com/file1.js", "Script", "Request") {
		t.Error("expected wildcard pattern to match")
	}
	if b.shouldPauseFetchRequest("s1", "https://cdn.example.com/file12.js", "Script", "Request") {
		t.Error("? must match exactly one character")
	}
	if b.shouldPauseFetchRequest("s1", "https://cdn.example.com/file1.js", "Script", "Response") {
		t.Error("request-stage pattern must not match a response-stage event")
	}
}

func TestFetchEnableWithoutPatternsMatchesAll(t *testing.T) {
	b, _ := newTestBridge()
	b.setFetchPatterns("s1", nil)

	if !b.shouldPauseFetchRequest("s1", "https://example.com/", "Document", "Request") {
		t.Error("Fetch.enable without patterns should pause every request")
	}
}

func TestNetworkRequestContinuesNonMatchingFetchPattern(t *testing.T) {
	b, mb := newTestBridge()
	b.sessions.Add(&cdp.SessionInfo{
		SessionID:        "cdp-s1",
		JugglerSessionID: "jug-s1",
		TargetID:         "target-1",
		FrameID:          "frame-1",
		Type:             "page",
	})
	patterns, err := compileFetchPatterns([]json.RawMessage{
		json.RawMessage(`{"urlPattern":"http://hermes-dialog-bridge.invalid/*"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	b.setFetchPatterns("cdp-s1", patterns)
	b.SetupEventSubscriptions()

	mb.mu.Lock()
	handler := mb.handlers["Network.requestWillBeSent"][0]
	mb.mu.Unlock()
	handler("jug-s1", json.RawMessage(`{
		"requestId":"request-1",
		"url":"https://github.com/",
		"method":"GET",
		"headers":[],
		"frameId":"frame-1",
		"navigationId":"navigation-1",
		"isIntercepted":true,
		"cause":"TYPE_DOCUMENT",
		"internalCause":"TYPE_DOCUMENT"
	}`))

	var calls []mockCall
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		calls = mb.CallsForMethod("Network.resumeInterceptedRequest")
		if len(calls) > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(calls) != 1 {
		t.Fatalf("continue calls = %d, want 1", len(calls))
	}
	var params map[string]interface{}
	if err := json.Unmarshal(calls[0].Params, &params); err != nil {
		t.Fatal(err)
	}
	if params["requestId"] != "request-1" {
		t.Errorf("requestId = %v, want request-1", params["requestId"])
	}
}

func TestFetchDisableClearsPatterns(t *testing.T) {
	b, _ := newTestBridge()
	b.setFetchPatterns("s1", []fetchRequestPattern{{
		URLPattern: "https://only.example/*",
		urlRegexp:  regexp.MustCompile(`^https://only\.example/.*$`),
	}})

	msg := &cdp.Message{ID: 1, Method: "Fetch.disable", SessionID: "s1"}
	if _, cdpErr := b.handleFetch(nil, msg); cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}
	if !b.shouldPauseFetchRequest("s1", "https://other.example/", "Document", "Request") {
		t.Error("disabled Fetch state must not filter interception requested elsewhere")
	}
}

func TestFetchEnableWithSession(t *testing.T) {
	b, mb := newTestBridge()
	b.sessions.Add(&cdp.SessionInfo{
		SessionID:        "s1",
		BrowserContextID: "ctx-1",
		TargetID:         "t1",
	})

	msg := &cdp.Message{
		ID:        1,
		Method:    "Fetch.enable",
		SessionID: "s1",
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}

	last, _ := mb.LastCall()
	var params map[string]interface{}
	json.Unmarshal(last.Params, &params)
	if params["browserContextId"] != "ctx-1" {
		t.Errorf("browserContextId = %v, want ctx-1", params["browserContextId"])
	}
}

func TestFetchDisable(t *testing.T) {
	b, mb := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.disable",
	}

	result, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}
	if string(result) != "{}" {
		t.Errorf("result = %s, want {}", string(result))
	}

	last, _ := mb.LastCall()
	if last.Method != "Browser.setRequestInterception" {
		t.Errorf("method = %q, want Browser.setRequestInterception", last.Method)
	}

	var params map[string]interface{}
	json.Unmarshal(last.Params, &params)
	if params["enabled"] != false {
		t.Errorf("enabled = %v, want false", params["enabled"])
	}
}

func TestFetchDisableWithSession(t *testing.T) {
	b, mb := newTestBridge()
	b.sessions.Add(&cdp.SessionInfo{
		SessionID:        "s2",
		BrowserContextID: "ctx-2",
		TargetID:         "t2",
	})

	msg := &cdp.Message{
		ID:        1,
		Method:    "Fetch.disable",
		SessionID: "s2",
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}

	last, _ := mb.LastCall()
	var params map[string]interface{}
	json.Unmarshal(last.Params, &params)
	if params["browserContextId"] != "ctx-2" {
		t.Errorf("browserContextId = %v, want ctx-2", params["browserContextId"])
	}
}

func TestFetchContinueRequest_HeaderConversion(t *testing.T) {
	b, mb := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.continueRequest",
		Params: json.RawMessage(`{
			"requestId": "req-1",
			"headers": [
				{"name": "Content-Type", "value": "application/json"},
				{"name": "Authorization", "value": "Bearer tok"}
			]
		}`),
	}

	result, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}
	if string(result) != "{}" {
		t.Errorf("result = %s, want {}", string(result))
	}

	last, _ := mb.LastCall()
	if last.Method != "Network.resumeInterceptedRequest" {
		t.Errorf("method = %q, want Network.resumeInterceptedRequest", last.Method)
	}

	var params map[string]interface{}
	json.Unmarshal(last.Params, &params)

	if params["requestId"] != "req-1" {
		t.Errorf("requestId = %v, want req-1", params["requestId"])
	}

	// Headers are now [{name, value}] array format for Juggler
	headers, ok := params["headers"].([]interface{})
	if !ok {
		t.Fatalf("headers not an array: %T", params["headers"])
	}
	if len(headers) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(headers))
	}
	h0 := headers[0].(map[string]interface{})
	if h0["name"] != "Content-Type" || h0["value"] != "application/json" {
		t.Errorf("header[0] = %v, want Content-Type: application/json", h0)
	}
}

func TestFetchContinueRequestUsesJugglerPageSession(t *testing.T) {
	b, mb := newTestBridge()
	b.sessions.Add(&cdp.SessionInfo{
		SessionID:        "cdp-s1",
		JugglerSessionID: "jug-s1",
		TargetID:         "target-1",
		Type:             "page",
	})

	msg := &cdp.Message{
		ID:        1,
		Method:    "Fetch.continueRequest",
		SessionID: "cdp-s1",
		Params:    json.RawMessage(`{"requestId":"request-1"}`),
	}
	if _, cdpErr := b.handleFetch(nil, msg); cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}

	last, err := mb.LastCall()
	if err != nil {
		t.Fatal(err)
	}
	if last.Method != "Network.resumeInterceptedRequest" {
		t.Errorf("method = %q, want Network.resumeInterceptedRequest", last.Method)
	}
	if last.SessionID != "jug-s1" {
		t.Errorf("session = %q, want jug-s1", last.SessionID)
	}
}

func TestFetchContinueRequest_URLAndMethodOverrides(t *testing.T) {
	b, mb := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.continueRequest",
		Params: json.RawMessage(`{
			"requestId": "req-2",
			"url": "https://override.example.com",
			"method": "POST"
		}`),
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}

	last, _ := mb.LastCall()
	var params map[string]interface{}
	json.Unmarshal(last.Params, &params)

	if params["url"] != "https://override.example.com" {
		t.Errorf("url = %v, want https://override.example.com", params["url"])
	}
	if params["method"] != "POST" {
		t.Errorf("method = %v, want POST", params["method"])
	}
}

func TestFetchContinueRequest_MinimalParams(t *testing.T) {
	b, mb := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.continueRequest",
		Params: json.RawMessage(`{"requestId": "req-3"}`),
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}

	last, _ := mb.LastCall()
	var params map[string]interface{}
	json.Unmarshal(last.Params, &params)

	if params["requestId"] != "req-3" {
		t.Errorf("requestId = %v, want req-3", params["requestId"])
	}
	if _, ok := params["url"]; ok {
		t.Error("url should not be set for minimal params")
	}
	if _, ok := params["method"]; ok {
		t.Error("method should not be set for minimal params")
	}
	if _, ok := params["headers"]; ok {
		t.Error("headers should not be set for minimal params")
	}
}

func TestFetchContinueRequest_InvalidParams(t *testing.T) {
	b, _ := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.continueRequest",
		Params: json.RawMessage(`not-json`),
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr == nil {
		t.Fatal("expected error for invalid params")
	}
	if cdpErr.Code != -32602 {
		t.Errorf("error code = %d, want -32602", cdpErr.Code)
	}
}

func TestFetchFulfillRequest(t *testing.T) {
	b, mb := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.fulfillRequest",
		Params: json.RawMessage(`{
			"requestId": "req-f1",
			"responseCode": 200,
			"responseHeaders": [
				{"name": "Content-Type", "value": "text/html"}
			],
			"body": "PGh0bWw+PC9odG1sPg=="
		}`),
	}

	result, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}
	if string(result) != "{}" {
		t.Errorf("result = %s, want {}", string(result))
	}

	last, _ := mb.LastCall()
	if last.Method != "Network.fulfillInterceptedRequest" {
		t.Errorf("method = %q, want Network.fulfillInterceptedRequest", last.Method)
	}

	var params map[string]interface{}
	json.Unmarshal(last.Params, &params)

	if params["requestId"] != "req-f1" {
		t.Errorf("requestId = %v, want req-f1", params["requestId"])
	}
	if params["status"] != float64(200) {
		t.Errorf("status = %v, want 200", params["status"])
	}
	if params["statusText"] != "OK" {
		t.Errorf("statusText = %v, want OK", params["statusText"])
	}
	if params["base64body"] != "PGh0bWw+PC9odG1sPg==" {
		t.Errorf("base64body = %v, want PGh0bWw+PC9odG1sPg==", params["base64body"])
	}
}

func TestFetchFulfillRequest_CustomStatusText(t *testing.T) {
	b, mb := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.fulfillRequest",
		Params: json.RawMessage(`{
			"requestId": "req-f2",
			"responseCode": 299,
			"responsePhrase": "Custom Status",
			"responseHeaders": [],
			"body": ""
		}`),
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}

	last, _ := mb.LastCall()
	var params map[string]interface{}
	json.Unmarshal(last.Params, &params)

	if params["statusText"] != "Custom Status" {
		t.Errorf("statusText = %v, want Custom Status", params["statusText"])
	}
}

func TestFetchFulfillRequest_StatusTextGeneration(t *testing.T) {
	codes := map[int]string{
		200: "OK",
		201: "Created",
		204: "No Content",
		301: "Moved Permanently",
		302: "Found",
		304: "Not Modified",
		400: "Bad Request",
		401: "Unauthorized",
		403: "Forbidden",
		404: "Not Found",
		405: "Method Not Allowed",
		500: "Internal Server Error",
		502: "Bad Gateway",
		503: "Service Unavailable",
		999: "OK", // unknown defaults to OK
	}

	for code, wantText := range codes {
		got := httpStatusText(code)
		if got != wantText {
			t.Errorf("httpStatusText(%d) = %q, want %q", code, got, wantText)
		}
	}
}

func TestFetchFulfillRequest_InvalidParams(t *testing.T) {
	b, _ := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.fulfillRequest",
		Params: json.RawMessage(`invalid`),
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr == nil {
		t.Fatal("expected error for invalid params")
	}
	if cdpErr.Code != -32602 {
		t.Errorf("error code = %d, want -32602", cdpErr.Code)
	}
}

func TestFetchFailRequest_AllErrorReasons(t *testing.T) {
	reasons := map[string]string{
		"Failed":               "failed",
		"Aborted":              "aborted",
		"TimedOut":             "timedout",
		"AccessDenied":         "accessdenied",
		"ConnectionClosed":     "connectionclosed",
		"ConnectionReset":      "connectionreset",
		"ConnectionRefused":    "connectionrefused",
		"ConnectionAborted":    "connectionaborted",
		"ConnectionFailed":     "connectionfailed",
		"NameNotResolved":      "namenotresolved",
		"InternetDisconnected": "internetdisconnected",
		"AddressUnreachable":   "addressunreachable",
		"BlockedByClient":      "blockedbyclient",
		"BlockedByResponse":    "blockedbyresponse",
	}

	for cdpReason, wantCode := range reasons {
		t.Run(cdpReason, func(t *testing.T) {
			b, mb := newTestBridge()

			params, _ := json.Marshal(map[string]string{
				"requestId":   "req-fail",
				"errorReason": cdpReason,
			})

			msg := &cdp.Message{
				ID:     1,
				Method: "Fetch.failRequest",
				Params: params,
			}

			result, cdpErr := b.handleFetch(nil, msg)
			if cdpErr != nil {
				t.Fatalf("unexpected error: %s", cdpErr.Message)
			}
			if string(result) != "{}" {
				t.Errorf("result = %s, want {}", string(result))
			}

			last, _ := mb.LastCall()
			if last.Method != "Network.abortInterceptedRequest" {
				t.Errorf("method = %q, want Network.abortInterceptedRequest", last.Method)
			}

			var p map[string]interface{}
			json.Unmarshal(last.Params, &p)
			if p["errorCode"] != wantCode {
				t.Errorf("errorCode = %v, want %s", p["errorCode"], wantCode)
			}
		})
	}
}

func TestFetchFailRequest_UnknownReason(t *testing.T) {
	got := mapErrorReason("SomethingNew")
	if got != "failed" {
		t.Errorf("mapErrorReason(SomethingNew) = %q, want failed", got)
	}
}

func TestFetchFailRequest_InvalidParams(t *testing.T) {
	b, _ := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.failRequest",
		Params: json.RawMessage(`bad`),
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr == nil {
		t.Fatal("expected error for invalid params")
	}
	if cdpErr.Code != -32602 {
		t.Errorf("error code = %d, want -32602", cdpErr.Code)
	}
}

func TestFetchGetResponseBody_Base64(t *testing.T) {
	b, mb := newTestBridge()

	// Binary data that is not valid UTF-8 text (contains null bytes)
	binaryData := []byte{0x00, 0x01, 0x02, 0xFF}
	b64 := base64.StdEncoding.EncodeToString(binaryData)

	resp, _ := json.Marshal(map[string]string{"base64body": b64})
	mb.SetResponse("", "Network.getResponseBody", resp, nil)

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.getResponseBody",
		Params: json.RawMessage(`{"requestId":"req-body-1"}`),
	}

	result, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}

	var res struct {
		Body          string `json:"body"`
		Base64Encoded bool   `json:"base64Encoded"`
	}
	json.Unmarshal(result, &res)

	if !res.Base64Encoded {
		t.Error("expected base64Encoded=true for binary data")
	}
	if res.Body != b64 {
		t.Errorf("body = %q, want %q", res.Body, b64)
	}
}

func TestFetchGetResponseBody_UTF8Text(t *testing.T) {
	b, mb := newTestBridge()

	textData := "Hello, World!"
	b64 := base64.StdEncoding.EncodeToString([]byte(textData))

	resp, _ := json.Marshal(map[string]string{"base64body": b64})
	mb.SetResponse("", "Network.getResponseBody", resp, nil)

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.getResponseBody",
		Params: json.RawMessage(`{"requestId":"req-body-2"}`),
	}

	result, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}

	var res struct {
		Body          string `json:"body"`
		Base64Encoded bool   `json:"base64Encoded"`
	}
	json.Unmarshal(result, &res)

	if res.Base64Encoded {
		t.Error("expected base64Encoded=false for UTF-8 text")
	}
	if res.Body != textData {
		t.Errorf("body = %q, want %q", res.Body, textData)
	}
}

func TestFetchGetResponseBody_InvalidParams(t *testing.T) {
	b, _ := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.getResponseBody",
		Params: json.RawMessage(`nope`),
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr == nil {
		t.Fatal("expected error for invalid params")
	}
	if cdpErr.Code != -32602 {
		t.Errorf("error code = %d, want -32602", cdpErr.Code)
	}
}

func TestFetchContinueWithAuth_ProvideCredentials(t *testing.T) {
	b, mb := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.continueWithAuth",
		Params: json.RawMessage(`{
			"requestId": "req-auth-1",
			"authChallengeResponse": {
				"response": "ProvideCredentials",
				"username": "user1",
				"password": "pass1"
			}
		}`),
	}

	result, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}
	if string(result) != "{}" {
		t.Errorf("result = %s, want {}", string(result))
	}

	last, _ := mb.LastCall()
	if last.Method != "Browser.handleAuthRequest" {
		t.Errorf("method = %q, want Browser.handleAuthRequest", last.Method)
	}

	var params map[string]interface{}
	json.Unmarshal(last.Params, &params)

	if params["action"] != "provideCredentials" {
		t.Errorf("action = %v, want provideCredentials", params["action"])
	}
	if params["username"] != "user1" {
		t.Errorf("username = %v, want user1", params["username"])
	}
	if params["password"] != "pass1" {
		t.Errorf("password = %v, want pass1", params["password"])
	}
}

func TestFetchContinueWithAuth_CancelAuth(t *testing.T) {
	b, mb := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.continueWithAuth",
		Params: json.RawMessage(`{
			"requestId": "req-auth-2",
			"authChallengeResponse": {"response": "CancelAuth"}
		}`),
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}

	last, _ := mb.LastCall()
	var params map[string]interface{}
	json.Unmarshal(last.Params, &params)

	if params["action"] != "cancel" {
		t.Errorf("action = %v, want cancel", params["action"])
	}
}

func TestFetchContinueWithAuth_Default(t *testing.T) {
	b, mb := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.continueWithAuth",
		Params: json.RawMessage(`{
			"requestId": "req-auth-3",
			"authChallengeResponse": {"response": "Default"}
		}`),
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr != nil {
		t.Fatalf("unexpected error: %s", cdpErr.Message)
	}

	last, _ := mb.LastCall()
	var params map[string]interface{}
	json.Unmarshal(last.Params, &params)

	if params["action"] != "default" {
		t.Errorf("action = %v, want default", params["action"])
	}
}

func TestFetchContinueWithAuth_InvalidParams(t *testing.T) {
	b, _ := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.continueWithAuth",
		Params: json.RawMessage(`bad-json`),
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr == nil {
		t.Fatal("expected error for invalid params")
	}
	if cdpErr.Code != -32602 {
		t.Errorf("error code = %d, want -32602", cdpErr.Code)
	}
}

func TestFetchUnknownMethod(t *testing.T) {
	b, _ := newTestBridge()

	msg := &cdp.Message{
		ID:     1,
		Method: "Fetch.doesNotExist",
		Params: json.RawMessage(`{}`),
	}

	_, cdpErr := b.handleFetch(nil, msg)
	if cdpErr == nil {
		t.Fatal("expected error for unknown method")
	}
	if cdpErr.Code != -32601 {
		t.Errorf("error code = %d, want -32601", cdpErr.Code)
	}
}

func TestIsUTF8Text(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{"plain ascii", []byte("hello"), true},
		{"utf8 multibyte", []byte("cafe\xc3\xa9"), true},
		{"contains null", []byte("hel\x00lo"), false},
		{"empty", []byte{}, true},
		{"just null", []byte{0x00}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isUTF8Text(tt.data)
			if got != tt.want {
				t.Errorf("isUTF8Text(%v) = %v, want %v", tt.data, got, tt.want)
			}
		})
	}
}
