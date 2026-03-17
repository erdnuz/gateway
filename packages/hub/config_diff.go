package hub

import (
	"fmt"
	"reflect"
	"sort"

	"gateway/packages/common/types"
)

type configUpdateAnalysis struct {
	ChangedSections          []string                        `json:"changed_sections"`
	Invalidations            types.ConfigInvalidations       `json:"invalidations"`
	Warnings                 []string                        `json:"warnings,omitempty"`
	ImpactedPrefixes         []string                        `json:"impacted_prefixes,omitempty"`
	ImpactedServices         []types.ConfigInvalidationScope `json:"impacted_services,omitempty"`
	HasQuotaReductionWarning bool                            `json:"has_quota_reduction_warning"`
}

func analyzeConfigUpdate(current, next *types.GatewayConfig) configUpdateAnalysis {
	if next == nil {
		return configUpdateAnalysis{}
	}
	if current == nil {
		return configUpdateAnalysis{
			ChangedSections: []string{"initial_load"},
		}
	}

	sections := map[string]struct{}{}
	invalidationSet := map[string]types.ConfigInvalidationScope{}
	prefixSet := map[string]struct{}{}
	serviceSet := map[string]types.ConfigInvalidationScope{}
	warnings := make([]string, 0, 2)
	hasQuotaReduction := false

	currentByPrefix := make(map[string]types.PrefixConfig, len(current.Prefixes))
	for _, p := range current.Prefixes {
		currentByPrefix[p.Prefix] = p
	}
	nextByPrefix := make(map[string]types.PrefixConfig, len(next.Prefixes))
	for _, p := range next.Prefixes {
		nextByPrefix[p.Prefix] = p
	}

	for prefix, oldPrefix := range currentByPrefix {
		newPrefix, exists := nextByPrefix[prefix]
		if !exists {
			sections["prefixes"] = struct{}{}
			addInvalidationScope(invalidationSet, types.ConfigInvalidationScope{Prefix: prefix, Mode: "mark-expire", Reason: "prefix_removed"})
			prefixSet[prefix] = struct{}{}
			continue
		}

		if oldPrefix.QuotaPeriod != newPrefix.QuotaPeriod || !reflect.DeepEqual(oldPrefix.Leasing, newPrefix.Leasing) {
			sections["prefixes"] = struct{}{}
			prefixSet[prefix] = struct{}{}
		}

		oldServices := make(map[string]types.ServiceConfig, len(oldPrefix.Services))
		for _, svc := range oldPrefix.Services {
			oldServices[svc.ServiceID] = svc
		}
		newServices := make(map[string]types.ServiceConfig, len(newPrefix.Services))
		for _, svc := range newPrefix.Services {
			newServices[svc.ServiceID] = svc
		}

		for serviceID, oldSvc := range oldServices {
			newSvc, serviceExists := newServices[serviceID]
			if !serviceExists {
				sections["services"] = struct{}{}
				scope := types.ConfigInvalidationScope{Prefix: prefix, ServiceID: serviceID, Mode: "mark-expire", Reason: "service_removed"}
				addInvalidationScope(invalidationSet, scope)
				serviceSet[scopeKey(scope)] = scope
				prefixSet[prefix] = struct{}{}
				continue
			}

			if !reflect.DeepEqual(oldSvc.Cache, newSvc.Cache) {
				sections["cache"] = struct{}{}
				scope := types.ConfigInvalidationScope{Prefix: prefix, ServiceID: serviceID, Reason: "cache_config_changed"}
				addInvalidationScope(invalidationSet, scope)
				serviceSet[scopeKey(scope)] = scope
				prefixSet[prefix] = struct{}{}
			}

			if !reflect.DeepEqual(oldSvc.Transform, newSvc.Transform) || oldSvc.TargetURL != newSvc.TargetURL {
				sections["services"] = struct{}{}
				scope := types.ConfigInvalidationScope{Prefix: prefix, ServiceID: serviceID, Reason: "service_behavior_changed"}
				addInvalidationScope(invalidationSet, scope)
				serviceSet[scopeKey(scope)] = scope
				prefixSet[prefix] = struct{}{}
			}

			if !reflect.DeepEqual(oldSvc.Tiers, newSvc.Tiers) || !reflect.DeepEqual(oldSvc.SafetyTier, newSvc.SafetyTier) {
				sections["tiers"] = struct{}{}
				scope := types.ConfigInvalidationScope{Prefix: prefix, ServiceID: serviceID, Reason: "tiers_changed"}
				addInvalidationScope(invalidationSet, scope)
				serviceSet[scopeKey(scope)] = scope
				prefixSet[prefix] = struct{}{}
				if hasQuotaReductionBetween(oldSvc, newSvc) {
					hasQuotaReduction = true
				}
			}

			if !reflect.DeepEqual(oldSvc.Analytics, newSvc.Analytics) {
				sections["analytics"] = struct{}{}
			}
		}

		for serviceID := range newServices {
			if _, exists := oldServices[serviceID]; !exists {
				sections["services"] = struct{}{}
				prefixSet[prefix] = struct{}{}
			}
		}
	}

	for prefix := range nextByPrefix {
		if _, exists := currentByPrefix[prefix]; !exists {
			sections["prefixes"] = struct{}{}
		}
	}

	if !reflect.DeepEqual(current.Runtime, next.Runtime) {
		sections["runtime"] = struct{}{}
	}

	if hasQuotaReduction {
		warnings = append(warnings, "quota reductions detected; if live usage is already above new quota, requests are denied until counters fall under quota or the window resets")
	}

	changedSections := make([]string, 0, len(sections))
	for section := range sections {
		changedSections = append(changedSections, section)
	}
	sort.Strings(changedSections)

	responseScopes := make([]types.ConfigInvalidationScope, 0, len(invalidationSet))
	for _, scope := range invalidationSet {
		responseScopes = append(responseScopes, scope)
	}
	sort.Slice(responseScopes, func(i, j int) bool {
		if responseScopes[i].Prefix == responseScopes[j].Prefix {
			return responseScopes[i].ServiceID < responseScopes[j].ServiceID
		}
		return responseScopes[i].Prefix < responseScopes[j].Prefix
	})

	impactedPrefixes := make([]string, 0, len(prefixSet))
	for prefix := range prefixSet {
		impactedPrefixes = append(impactedPrefixes, prefix)
	}
	sort.Strings(impactedPrefixes)

	impactedServices := make([]types.ConfigInvalidationScope, 0, len(serviceSet))
	for _, scope := range serviceSet {
		if scope.ServiceID != "" {
			impactedServices = append(impactedServices, scope)
		}
	}
	sort.Slice(impactedServices, func(i, j int) bool {
		if impactedServices[i].Prefix == impactedServices[j].Prefix {
			return impactedServices[i].ServiceID < impactedServices[j].ServiceID
		}
		return impactedServices[i].Prefix < impactedServices[j].Prefix
	})

	return configUpdateAnalysis{
		ChangedSections: changedSections,
		Invalidations: types.ConfigInvalidations{
			ResponseCache: responseScopes,
		},
		Warnings:                 warnings,
		ImpactedPrefixes:         impactedPrefixes,
		ImpactedServices:         impactedServices,
		HasQuotaReductionWarning: hasQuotaReduction,
	}
}

func hasQuotaReductionBetween(oldSvc, newSvc types.ServiceConfig) bool {
	oldTiers := map[string]uint32{}
	for _, tier := range oldSvc.Tiers {
		oldTiers[tier.TierID] = tier.Quota
	}
	for _, tier := range newSvc.Tiers {
		oldQuota, exists := oldTiers[tier.TierID]
		if exists && tier.Quota < oldQuota {
			return true
		}
	}
	if oldSvc.SafetyTier != nil && newSvc.SafetyTier != nil && newSvc.SafetyTier.Quota < oldSvc.SafetyTier.Quota {
		return true
	}
	return false
}

func addInvalidationScope(set map[string]types.ConfigInvalidationScope, scope types.ConfigInvalidationScope) {
	if scope.Prefix == "" {
		return
	}
	if existing, ok := set[scopeKey(scope)]; ok {
		if existing.Mode == "" && scope.Mode != "" {
			existing.Mode = scope.Mode
		}
		if existing.Reason == "" && scope.Reason != "" {
			existing.Reason = scope.Reason
		}
		set[scopeKey(scope)] = existing
		return
	}
	set[scopeKey(scope)] = scope
}

func scopeKey(scope types.ConfigInvalidationScope) string {
	return fmt.Sprintf("%s|%s", scope.Prefix, scope.ServiceID)
}
