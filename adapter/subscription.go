package adapter

import (
	"context"
	"errors"
	"time"

	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
)

// SubscriptionHostOptions supplies immutable static dependencies for validating
// a dynamic provider update against the rest of the running configuration.
type SubscriptionHostOptions struct{ Options option.Options }

// SubscriptionScopeOptions retains this candidate's declarations for validating
// source-only references after nested providers become available.
type SubscriptionScopeOptions struct{ Options option.Options }

type SelectableOutboundGroup interface {
	OutboundGroup
	SelectOutbound(tag string) bool
}

// OutboundProvider exposes namespaced, stable handles, never its live implementation objects.
type OutboundProvider interface {
	ProviderOutbounds() []Outbound
	ProviderOutbound(tag string) (Outbound, bool)
}

// ProvidedOutbound is a stable handle that can be resolved before its first
// snapshot is loaded. The provider, rather than the member, has a lifecycle.
type ProvidedOutbound interface {
	Outbound
	ProviderTag() string
	Available() bool
}

type SubscriptionStatus struct {
	Name             string            `json:"name"`
	Ready            bool              `json:"ready"`
	UpdatedAt        time.Time         `json:"updatedAt"`
	LastError        string            `json:"lastError,omitempty"`
	Generation       uint64            `json:"generation"`
	TestURL          string            `json:"testUrl"`
	SubscriptionInfo *SubscriptionInfo `json:"subscriptionInfo,omitempty"`
}

type SubscriptionInfo struct {
	Upload   uint64 `json:"Upload"`
	Download uint64 `json:"Download"`
	Total    uint64 `json:"Total"`
	Expire   uint64 `json:"Expire"`
}

type Subscription interface {
	OutboundGroup
	OutboundProvider
	URLTestGroup
	Restore() error
	Refresh(context.Context) error
	Status() SubscriptionStatus
}

// Subscription sources share provider lifecycle machinery but are not routing
// targets. Group import and provider APIs may access them explicitly.
func IsSubscriptionSource(outbound Outbound) bool {
	if source, ok := outbound.(interface{ SubscriptionSourceOnly() bool }); ok {
		return source.SubscriptionSourceOnly()
	}
	return false
}

// InitializeSubscriptions restores every cache before attempting network access. A
// successful pass can make another subscription's download path available.
type SubscriptionBootstrapTask struct {
	Name string
	Run  func(context.Context) error
	// Only subscription downloads can be deferred. Required resources and
	// invalid dependency graphs must still prevent publication/startup.
	AllowUnavailable bool
}

// SubscriptionDownloadError distinguishes an unavailable remote source from
// invalid configuration or a rejected candidate resource graph.
type SubscriptionDownloadError struct{ Err error }

func (e *SubscriptionDownloadError) Error() string { return e.Err.Error() }
func (e *SubscriptionDownloadError) Unwrap() error { return e.Err }

func IsSubscriptionDownloadError(err error) bool {
	var downloadError *SubscriptionDownloadError
	return errors.As(err, &downloadError)
}

func InitializeSubscriptions(ctx context.Context, manager OutboundManager, tasks ...SubscriptionBootstrapTask) error {
	return initializeSubscriptions(ctx, manager, false, tasks...)
}

// Candidate scopes may refer to another provider that has not loaded yet. The
// complete tree is checked strictly before publishing an update or serving traffic.
func InitializeSubscriptionScope(ctx context.Context, manager OutboundManager, tasks ...SubscriptionBootstrapTask) error {
	return initializeSubscriptions(ctx, manager, true, tasks...)
}

func initializeSubscriptions(ctx context.Context, manager OutboundManager, allowPending bool, tasks ...SubscriptionBootstrapTask) error {
	pending := append([]SubscriptionBootstrapTask(nil), tasks...)
	for _, outbound := range manager.Outbounds() {
		if subscription, ok := outbound.(Subscription); ok {
			_ = subscription.Restore() // Refresh reports the final error if no cache works.
			if !subscription.Status().Ready {
				pending = append(pending, SubscriptionBootstrapTask{Name: subscription.Tag(), Run: subscription.Refresh, AllowUnavailable: true})
			}
		}
	}
	for len(pending) > 0 {
		var next []SubscriptionBootstrapTask
		var failures []error
		for _, task := range pending {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := task.Run(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				next = append(next, task)
				if !task.AllowUnavailable || !IsSubscriptionDownloadError(err) {
					failures = append(failures, E.Cause(err, "subscription resource[", task.Name, "]"))
				}
			}
		}
		if len(next) == len(pending) {
			if len(failures) > 0 {
				return E.Cause(E.Errors(failures...), "initialize subscriptions")
			}
			// Keep unavailable providers registered with empty members. Their
			// background loops will retry, then notify every importing group.
			break
		}
		pending = next
	}
	return ValidateSubscriptionDependencies(ctx, manager, allowPending)
}

func ValidateSubscriptionDependencies(ctx context.Context, manager OutboundManager, allowPending bool) error {
	isSource := func(tag string) bool {
		outbound, found := manager.Outbound(tag)
		return found && IsSubscriptionSource(outbound)
	}
	if host := service.PtrFromContext[SubscriptionHostOptions](ctx); host != nil {
		if err := option.ValidateSubscriptionReferences(host.Options, isSource); err != nil {
			return err
		}
	}
	if local := service.PtrFromContext[SubscriptionScopeOptions](ctx); local != nil {
		if err := option.ValidateSubscriptionReferences(local.Options, isSource); err != nil {
			return err
		}
	}
	outbounds := manager.Outbounds()
	if endpoints := service.FromContext[EndpointManager](ctx); endpoints != nil {
		for _, endpoint := range endpoints.Endpoints() {
			outbounds = append(outbounds, endpoint)
		}
	}
	for _, outbound := range outbounds {
		if validator, ok := outbound.(GroupImportValidator); ok {
			if err := validator.ValidateImports(allowPending); err != nil {
				return E.Cause(err, "group[", outbound.Tag(), "]")
			}
		}
		for _, tag := range outbound.Dependencies() {
			if imports, ok := outbound.(OptionalGroupImport); ok && imports.OptionalImportDependency(tag) {
				continue
			}
			dependency, found := manager.Outbound(tag)
			if !found {
				return E.New("subscription dependency not found: ", tag, " for ", outbound.Tag())
			}
			if IsSubscriptionSource(dependency) {
				importer, ok := outbound.(interface{ ImportsSource(string) bool })
				if !ok || !importer.ImportsSource(tag) {
					return E.New("subscription source ", tag, " is not dialable; use import in ", outbound.Tag())
				}
			}
			if provided, ok := dependency.(ProvidedOutbound); ok && !provided.Available() && !allowPending {
				return E.New("subscription member not found: ", tag, " for ", outbound.Tag())
			}
		}
	}
	if provided, ok := manager.Default().(ProvidedOutbound); ok && !provided.Available() && !allowPending {
		return E.New("default subscription member not found: ", provided.Tag())
	}
	if IsSubscriptionSource(manager.Default()) {
		return E.New("default outbound is a subscription source; use import")
	}
	return nil
}

// OutboundControl exposes administrative views without letting routing unwrap a
// provider's connection leases. Never use these views to route traffic.
type OutboundControl interface {
	GroupControl() (OutboundGroup, bool)
	SubscriptionControl() (Subscription, bool)
}

func OutboundGroupFor(outbound Outbound) (OutboundGroup, bool) {
	if group, ok := outbound.(OutboundGroup); ok {
		return group, true
	}
	if control, ok := outbound.(OutboundControl); ok {
		return control.GroupControl()
	}
	return nil, false
}

func SubscriptionFor(outbound Outbound) (Subscription, bool) {
	if subscription, ok := outbound.(Subscription); ok {
		return subscription, true
	}
	if control, ok := outbound.(OutboundControl); ok {
		return control.SubscriptionControl()
	}
	return nil, false
}
