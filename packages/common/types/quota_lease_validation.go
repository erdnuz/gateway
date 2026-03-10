package types

import "fmt"

// Validate performs request-level checks before lease grant logic.
func (r *QuotaLeaseRequest) Validate() error {
	if r == nil {
		return fmt.Errorf("request is nil")
	}
	if r.GetPrefix() == "" {
		return fmt.Errorf("prefix is required")
	}
	if r.GetServiceId() == "" {
		return fmt.Errorf("service_id is required")
	}
	if r.GetApiKey() == "" {
		return fmt.Errorf("api_key is required")
	}
	if r.GetRequestedTokens() <= 0 {
		return fmt.Errorf("requested_tokens must be > 0")
	}
	return nil
}
