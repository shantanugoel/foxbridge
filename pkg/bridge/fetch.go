package bridge

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/VulpineOS/foxbridge/pkg/cdp"
)

type fetchRequestPattern struct {
	URLPattern   string `json:"urlPattern"`
	ResourceType string `json:"resourceType"`
	RequestStage string `json:"requestStage"`

	urlRegexp *regexp.Regexp
}

func compileFetchPatterns(raw []json.RawMessage) ([]fetchRequestPattern, error) {
	patterns := make([]fetchRequestPattern, 0, len(raw))
	for _, value := range raw {
		var pattern fetchRequestPattern
		if err := json.Unmarshal(value, &pattern); err != nil {
			return nil, err
		}
		if pattern.URLPattern == "" {
			pattern.URLPattern = "*"
		}
		re, err := regexp.Compile(cdpURLPatternRegexp(pattern.URLPattern))
		if err != nil {
			return nil, err
		}
		pattern.urlRegexp = re
		patterns = append(patterns, pattern)
	}
	return patterns, nil
}

// cdpURLPatternRegexp converts the simple wildcard syntax used by CDP's
// Fetch.RequestPattern into a regular expression. '*' matches any number of
// characters, '?' matches one character, and '\\' escapes the next character.
func cdpURLPatternRegexp(pattern string) string {
	var result strings.Builder
	result.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*':
			result.WriteString(".*")
		case '?':
			result.WriteString(".")
		case '\\':
			if i+1 < len(pattern) {
				i++
				result.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
			} else {
				result.WriteString(`\\`)
			}
		default:
			result.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
		}
	}
	result.WriteString("$")
	return result.String()
}

func (b *Bridge) setFetchPatterns(sessionID string, patterns []fetchRequestPattern) {
	b.fetchPatternsMu.Lock()
	b.fetchPatterns[sessionID] = patterns
	b.fetchPatternsMu.Unlock()
}

func (b *Bridge) clearFetchPatterns(sessionID string) {
	b.fetchPatternsMu.Lock()
	delete(b.fetchPatterns, sessionID)
	b.fetchPatternsMu.Unlock()
}

func (b *Bridge) shouldPauseFetchRequest(sessionID, url, resourceType, requestStage string) bool {
	b.fetchPatternsMu.RLock()
	patterns, enabled := b.fetchPatterns[sessionID]
	if !enabled && sessionID != "" {
		patterns, enabled = b.fetchPatterns[""]
	}
	b.fetchPatternsMu.RUnlock()

	// An enabled Fetch domain with no patterns means intercept everything.
	// If Fetch is not enabled here, preserve interception requested through
	// the legacy Network domain or directly by another bridge feature.
	if !enabled || len(patterns) == 0 {
		return true
	}

	for _, pattern := range patterns {
		stage := pattern.RequestStage
		if stage == "" {
			stage = "Request"
		}
		if !strings.EqualFold(stage, requestStage) {
			continue
		}
		if pattern.ResourceType != "" && !strings.EqualFold(pattern.ResourceType, resourceType) {
			continue
		}
		if pattern.urlRegexp.MatchString(url) {
			return true
		}
	}
	return false
}

// callFetchBackend translates Fetch request actions to the backend's native
// interception API. Current Juggler exposes actions on the page-scoped Network
// domain, while the BiDi adapter retains Foxbridge's browser-scoped shim.
func (b *Bridge) callFetchBackend(sessionID, jugglerMethod, bidiMethod string, params interface{}) (json.RawMessage, error) {
	if b.isBiDi {
		return b.callJuggler("", bidiMethod, params)
	}
	return b.callJuggler(sessionID, jugglerMethod, params)
}

func (b *Bridge) continueFetchRequest(sessionID, requestID string) error {
	_, err := b.callFetchBackend(
		sessionID,
		"Network.resumeInterceptedRequest",
		"Browser.continueInterceptedRequest",
		map[string]interface{}{"requestId": requestID},
	)
	return err
}

func (b *Bridge) handleFetch(conn *cdp.Connection, msg *cdp.Message) (json.RawMessage, *cdp.Error) {
	switch msg.Method {
	case "Fetch.enable":
		var params struct {
			Patterns           []json.RawMessage `json:"patterns"`
			HandleAuthRequests bool              `json:"handleAuthRequests"`
		}
		if msg.Params != nil {
			if err := json.Unmarshal(msg.Params, &params); err != nil {
				return nil, &cdp.Error{Code: -32602, Message: "invalid params"}
			}
		}
		patterns, err := compileFetchPatterns(params.Patterns)
		if err != nil {
			return nil, &cdp.Error{Code: -32602, Message: "invalid Fetch pattern"}
		}
		b.setFetchPatterns(msg.SessionID, patterns)

		jugglerParams := map[string]interface{}{
			"enabled": true,
		}
		if msg.SessionID != "" {
			if info, ok := b.sessions.Get(msg.SessionID); ok {
				b.setJugglerBrowserContext(jugglerParams, info.BrowserContextID)
			}
		}

		_, err = b.callJuggler("", "Browser.setRequestInterception", jugglerParams)
		if err != nil {
			b.clearFetchPatterns(msg.SessionID)
			return nil, &cdp.Error{Code: -32000, Message: err.Error()}
		}
		return json.RawMessage(`{}`), nil

	case "Fetch.disable":
		b.clearFetchPatterns(msg.SessionID)
		jugglerParams := map[string]interface{}{
			"enabled": false,
		}
		if msg.SessionID != "" {
			if info, ok := b.sessions.Get(msg.SessionID); ok {
				b.setJugglerBrowserContext(jugglerParams, info.BrowserContextID)
			}
		}

		_, err := b.callJuggler("", "Browser.setRequestInterception", jugglerParams)
		if err != nil {
			return nil, &cdp.Error{Code: -32000, Message: err.Error()}
		}
		return json.RawMessage(`{}`), nil

	case "Fetch.continueRequest":
		var params struct {
			RequestID         string        `json:"requestId"`
			URL               string        `json:"url"`
			Method            string        `json:"method"`
			PostData          string        `json:"postData"`
			Headers           []headerEntry `json:"headers"`
			InterceptResponse bool          `json:"interceptResponse"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, &cdp.Error{Code: -32602, Message: "invalid params"}
		}

		jugglerParams := map[string]interface{}{
			"requestId": params.RequestID,
		}
		if params.URL != "" {
			jugglerParams["url"] = params.URL
		}
		if params.Method != "" {
			jugglerParams["method"] = params.Method
		}
		if len(params.Headers) > 0 {
			// Juggler expects headers as [{name, value}] array, not a map
			headers := make([]map[string]string, len(params.Headers))
			for i, h := range params.Headers {
				headers[i] = map[string]string{"name": h.Name, "value": h.Value}
			}
			jugglerParams["headers"] = headers
		}
		if params.PostData != "" {
			jugglerParams["postData"] = params.PostData
		}

		_, err := b.callFetchBackend(msg.SessionID, "Network.resumeInterceptedRequest", "Browser.continueInterceptedRequest", jugglerParams)
		if err != nil {
			return nil, &cdp.Error{Code: -32000, Message: err.Error()}
		}
		return json.RawMessage(`{}`), nil

	case "Fetch.fulfillRequest":
		var params struct {
			RequestID             string        `json:"requestId"`
			ResponseCode          int           `json:"responseCode"`
			ResponseHeaders       []headerEntry `json:"responseHeaders"`
			BinaryResponseHeaders string        `json:"binaryResponseHeaders"`
			Body                  string        `json:"body"`
			ResponsePhrase        string        `json:"responsePhrase"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, &cdp.Error{Code: -32602, Message: "invalid params"}
		}

		statusText := params.ResponsePhrase
		if statusText == "" {
			statusText = httpStatusText(params.ResponseCode)
		}

		// Convert CDP header array to Juggler header array of {name, value} objects
		var headers []map[string]string
		for _, h := range params.ResponseHeaders {
			headers = append(headers, map[string]string{
				"name":  h.Name,
				"value": h.Value,
			})
		}

		// CDP sends body as base64-encoded string; Juggler expects the same
		jugglerParams := map[string]interface{}{
			"requestId":  params.RequestID,
			"status":     params.ResponseCode,
			"statusText": statusText,
			"headers":    headers,
			"base64body": params.Body,
		}

		_, err := b.callFetchBackend(msg.SessionID, "Network.fulfillInterceptedRequest", "Browser.fulfillInterceptedRequest", jugglerParams)
		if err != nil {
			return nil, &cdp.Error{Code: -32000, Message: err.Error()}
		}
		return json.RawMessage(`{}`), nil

	case "Fetch.failRequest":
		var params struct {
			RequestID   string `json:"requestId"`
			ErrorReason string `json:"errorReason"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, &cdp.Error{Code: -32602, Message: "invalid params"}
		}

		// Map CDP Network.ErrorReason to Juggler error code
		errorCode := mapErrorReason(params.ErrorReason)

		jugglerParams := map[string]interface{}{
			"requestId": params.RequestID,
			"errorCode": errorCode,
		}

		_, err := b.callFetchBackend(msg.SessionID, "Network.abortInterceptedRequest", "Browser.abortInterceptedRequest", jugglerParams)
		if err != nil {
			return nil, &cdp.Error{Code: -32000, Message: err.Error()}
		}
		return json.RawMessage(`{}`), nil

	case "Fetch.continueWithAuth":
		var params struct {
			RequestID             string `json:"requestId"`
			AuthChallengeResponse struct {
				Response string `json:"response"` // "Default", "CancelAuth", "ProvideCredentials"
				Username string `json:"username"`
				Password string `json:"password"`
			} `json:"authChallengeResponse"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, &cdp.Error{Code: -32602, Message: "invalid params"}
		}

		jugglerParams := map[string]interface{}{
			"requestId": params.RequestID,
		}

		switch params.AuthChallengeResponse.Response {
		case "ProvideCredentials":
			jugglerParams["action"] = "provideCredentials"
			jugglerParams["username"] = params.AuthChallengeResponse.Username
			jugglerParams["password"] = params.AuthChallengeResponse.Password
		case "CancelAuth":
			jugglerParams["action"] = "cancel"
		default:
			jugglerParams["action"] = "default"
		}

		_, err := b.callJuggler("", "Browser.handleAuthRequest", jugglerParams)
		if err != nil {
			return nil, &cdp.Error{Code: -32000, Message: err.Error()}
		}
		return json.RawMessage(`{}`), nil

	case "Fetch.getResponseBody":
		var params struct {
			RequestID string `json:"requestId"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, &cdp.Error{Code: -32602, Message: "invalid params"}
		}

		result, err := b.callFetchBackend(msg.SessionID, "Network.getResponseBody", "Browser.getResponseBody", map[string]interface{}{
			"requestId": params.RequestID,
		})
		if err != nil {
			return nil, &cdp.Error{Code: -32000, Message: err.Error()}
		}

		// Juggler returns {base64body: "..."}, CDP expects {body: "...", base64Encoded: bool}
		var jugglerResult struct {
			Base64Body string `json:"base64body"`
		}
		if err := json.Unmarshal(result, &jugglerResult); err != nil {
			return nil, &cdp.Error{Code: -32000, Message: "failed to parse response body"}
		}

		// Check if the body is valid UTF-8 text; if so, decode and return as plain text
		decoded, decodeErr := base64.StdEncoding.DecodeString(jugglerResult.Base64Body)
		if decodeErr == nil && isUTF8Text(decoded) {
			resp, _ := json.Marshal(map[string]interface{}{
				"body":          string(decoded),
				"base64Encoded": false,
			})
			return resp, nil
		}

		// Return as base64
		resp, _ := json.Marshal(map[string]interface{}{
			"body":          jugglerResult.Base64Body,
			"base64Encoded": true,
		})
		return resp, nil

	case "Fetch.continueResponse":
		// Continue with modified response — allows response body interception.
		// The response is forwarded to the page with optional modifications.
		var params struct {
			RequestID             string        `json:"requestId"`
			ResponseCode          int           `json:"responseCode"`
			ResponseHeaders       []headerEntry `json:"responseHeaders"`
			BinaryResponseHeaders string        `json:"binaryResponseHeaders"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, &cdp.Error{Code: -32602, Message: "invalid params"}
		}

		jugglerParams := map[string]interface{}{
			"requestId": params.RequestID,
		}
		if params.ResponseCode > 0 {
			jugglerParams["status"] = params.ResponseCode
		}
		if len(params.ResponseHeaders) > 0 {
			headers := make([]map[string]string, len(params.ResponseHeaders))
			for i, h := range params.ResponseHeaders {
				headers[i] = map[string]string{"name": h.Name, "value": h.Value}
			}
			jugglerParams["headers"] = headers
		}

		_, err := b.callFetchBackend(msg.SessionID, "Network.resumeInterceptedRequest", "Browser.continueInterceptedRequest", jugglerParams)
		if err != nil {
			return nil, &cdp.Error{Code: -32000, Message: err.Error()}
		}
		return json.RawMessage(`{}`), nil

	default:
		return nil, &cdp.Error{Code: -32601, Message: fmt.Sprintf("method not found: %s", msg.Method)}
	}
}

// headerEntry represents a CDP header entry with name/value pair.
type headerEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// isUTF8Text checks if the byte slice looks like valid UTF-8 text (no null bytes).
func isUTF8Text(data []byte) bool {
	for _, b := range data {
		if b == 0 {
			return false
		}
	}
	return true
}

// mapErrorReason converts a CDP Network.ErrorReason to a Juggler error code string.
func mapErrorReason(reason string) string {
	switch reason {
	case "Failed":
		return "failed"
	case "Aborted":
		return "aborted"
	case "TimedOut":
		return "timedout"
	case "AccessDenied":
		return "accessdenied"
	case "ConnectionClosed":
		return "connectionclosed"
	case "ConnectionReset":
		return "connectionreset"
	case "ConnectionRefused":
		return "connectionrefused"
	case "ConnectionAborted":
		return "connectionaborted"
	case "ConnectionFailed":
		return "connectionfailed"
	case "NameNotResolved":
		return "namenotresolved"
	case "InternetDisconnected":
		return "internetdisconnected"
	case "AddressUnreachable":
		return "addressunreachable"
	case "BlockedByClient":
		return "blockedbyclient"
	case "BlockedByResponse":
		return "blockedbyresponse"
	default:
		return "failed"
	}
}

// httpStatusText returns the standard status text for an HTTP status code.
func httpStatusText(code int) string {
	switch code {
	case 200:
		return "OK"
	case 201:
		return "Created"
	case 204:
		return "No Content"
	case 301:
		return "Moved Permanently"
	case 302:
		return "Found"
	case 304:
		return "Not Modified"
	case 400:
		return "Bad Request"
	case 401:
		return "Unauthorized"
	case 403:
		return "Forbidden"
	case 404:
		return "Not Found"
	case 405:
		return "Method Not Allowed"
	case 500:
		return "Internal Server Error"
	case 502:
		return "Bad Gateway"
	case 503:
		return "Service Unavailable"
	default:
		return "OK"
	}
}
