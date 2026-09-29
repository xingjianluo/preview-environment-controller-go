package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/environment"
)

// Client is a thin HTTP client for the controller API. The CLI uses it, and
// it reconstructs the same structured *environment.Error on failure, so the
// HTTP/CLI entries report exactly the same codes as the library entry.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// NewClient builds a client with sane defaults.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 10 * time.Second},
	}
}

// ApplyParams is the input to Client.Apply.
type ApplyParams struct {
	Name      string
	Target    string
	Holder    string
	TTL       time.Duration
	RequestID string
}

func (c *Client) Apply(ctx context.Context, p ApplyParams) (*environment.Environment, error) {
	body := map[string]string{
		"target": p.Target, "name": p.Name, "holder": p.Holder,
		"request_id": p.RequestID,
	}
	if p.TTL > 0 {
		body["ttl"] = p.TTL.String()
	}
	var out envResponse
	if err := c.do(ctx, http.MethodPost, "/v1/environments", body, &out, p.RequestID); err != nil {
		return nil, err
	}
	return fromDTO(out.Environment), nil
}

func (c *Client) Occupy(ctx context.Context, name, holder, requestID string) (*environment.Environment, error) {
	return c.holderAction(ctx, name, "occupy", holder, requestID)
}

func (c *Client) Release(ctx context.Context, name, holder, requestID string) (*environment.Environment, error) {
	return c.holderAction(ctx, name, "release", holder, requestID)
}

func (c *Client) ReleaseAll(ctx context.Context, name, requestID string) (*environment.Environment, error) {
	var out envResponse
	if err := c.do(ctx, http.MethodPost,
		fmt.Sprintf("/v1/environments/%s/release-all", name),
		map[string]string{"request_id": requestID}, &out, requestID); err != nil {
		return nil, err
	}
	return fromDTO(out.Environment), nil
}

func (c *Client) Reclaim(ctx context.Context, name, requestID string) (*environment.Environment, error) {
	var out envResponse
	if err := c.do(ctx, http.MethodPost,
		fmt.Sprintf("/v1/environments/%s/reclaim", name),
		map[string]string{"request_id": requestID}, &out, requestID); err != nil {
		return nil, err
	}
	return fromDTO(out.Environment), nil
}

// ReclaimExpired triggers one sweep over HTTP.
func (c *Client) ReclaimExpired(ctx context.Context) error {
	var out reclaimResponse
	return c.do(ctx, http.MethodPost, "/v1/reclaim-expired", struct{}{}, &out, "")
}

func (c *Client) List(ctx context.Context) ([]*environment.Environment, error) {
	var out listResponse
	if err := c.do(ctx, http.MethodGet, "/v1/environments", nil, &out, ""); err != nil {
		return nil, err
	}
	list := make([]*environment.Environment, 0, len(out.Environments))
	for i := range out.Environments {
		list = append(list, fromDTO(out.Environments[i]))
	}
	return list, nil
}

func (c *Client) Get(ctx context.Context, name string) (*environment.Environment, error) {
	var out envResponse
	if err := c.do(ctx, http.MethodGet,
		fmt.Sprintf("/v1/environments/%s", name), nil, &out, ""); err != nil {
		return nil, err
	}
	return fromDTO(out.Environment), nil
}

func (c *Client) holderAction(ctx context.Context, name, action, holder, requestID string) (*environment.Environment, error) {
	var out envResponse
	if err := c.do(ctx, http.MethodPost,
		fmt.Sprintf("/v1/environments/%s/%s", name, action),
		map[string]string{"holder": holder, "request_id": requestID},
		&out, requestID); err != nil {
		return nil, err
	}
	return fromDTO(out.Environment), nil
}

func (c *Client) do(ctx context.Context, method, path string, in any, out any, requestID string) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if requestID != "" {
		req.Header.Set("X-Request-Id", requestID)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return environment.NewError(environment.ErrCodeProvisioner,
			"controller unreachable: "+err.Error())
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		var dto errorDTO
		if json.Unmarshal(data, &dto) == nil && dto.Code != "" {
			return &environment.Error{
				Code: dto.Code, Message: dto.Message,
				Target: dto.Target, RequestID: dto.RequestID,
			}
		}
		return environment.NewError(environment.ErrCodeProvisioner,
			fmt.Sprintf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data))))
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func fromDTO(d envDTO) *environment.Environment {
	ttl, _ := time.ParseDuration(d.TTL)
	e := &environment.Environment{
		Name: d.Name, Target: d.Target, Status: d.Status,
		Port: d.Port, PID: d.PID, Endpoint: d.Endpoint, TTL: ttl,
		ExpiresAt: d.ExpiresAt, ReadyAt: d.ReadyAt,
		CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt,
		ClaimVersion: d.ClaimVersion, ReclaimErrs: d.ReclaimErrs,
		Holders: append([]string(nil), d.Holders...),
	}
	for _, ev := range d.Events {
		e.Events = append(e.Events, environment.Event{
			Time: ev.Time, From: ev.From, To: ev.To,
			Reason: ev.Reason, Detail: ev.Detail, RequestID: ev.RequestID,
		})
	}
	return e
}
