package option

import (
	"reflect"
	"slices"
	"strconv"

	C "github.com/sagernet/sing-box/constant"
	E "github.com/sagernet/sing/common/exceptions"
)

// ExpandSubscriptions reuses the subscription engine's lifecycle and dependency
// ordering, while retaining a strict distinction between sources and dialable
// outbounds. It operates on a copy and never rewrites the user's configuration.
func ExpandSubscriptions(options Options) (Options, error) {
	options.Outbounds = slices.Clone(options.Outbounds)
	for _, source := range options.Subscriptions {
		if source.Tag == "" {
			return options, E.New("missing subscription tag")
		}
		options.Outbounds = append(options.Outbounds, Outbound{
			Type: C.TypeSubscription, Tag: source.Tag,
			Options: &SubscriptionOutboundOptions{
				SourceOnly: true, URL: source.URL, HTTPClient: source.HTTPClient,
				BootstrapHTTPClient: source.BootstrapHTTPClient, CachePath: source.CachePath,
				UpdateInterval: source.UpdateInterval, DownloadTimeout: source.DownloadTimeout,
				MaxDownloadSize: source.MaxDownloadSize, MaxCacheSize: source.MaxCacheSize,
				DrainTimeout: source.DrainTimeout, Outbounds: source.Outbounds, Import: source.Import,
				OverrideDialer: source.OverrideDialer, TestURL: source.TestURL, TestInterval: source.TestInterval,
			},
		})
	}
	options.Subscriptions = nil
	if err := checkOutbounds(options.Outbounds, options.Endpoints); err != nil {
		return options, err
	}
	sources := make(map[string]bool)
	for _, outbound := range options.Outbounds {
		if source, ok := outbound.Options.(*SubscriptionOutboundOptions); ok && source.SourceOnly {
			sources[outbound.Tag] = true
		}
	}
	return options, ValidateSubscriptionReferences(options, func(tag string) bool { return sources[tag] })
}

// ValidateSubscriptionReferences checks resolved source identities as well as
// static declarations. Import is the sole operation that can consume a source.
func ValidateSubscriptionReferences(options Options, isSource func(string) bool) error {
	var validate func(reflect.Value, string) error
	validate = func(value reflect.Value, path string) error {
		if !value.IsValid() {
			return nil
		}
		if value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
			if value.IsNil() {
				return nil
			}
			return validate(value.Elem(), path)
		}
		// Imports are the only references allowed to consume a subscription source.
		if value.Type() == reflect.TypeFor[GroupImport]() {
			return nil
		}
		switch value.Kind() {
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				field := value.Type().Field(i)
				if !field.IsExported() {
					continue
				}
				v := value.Field(i)
				if field.Tag.Get("reference") == "outbound" {
					var check func(reflect.Value) error
					check = func(v reflect.Value) error {
						if v.Kind() == reflect.Pointer {
							if v.IsNil() {
								return nil
							}
							return check(v.Elem())
						}
						if v.Kind() == reflect.Slice {
							for j := 0; j < v.Len(); j++ {
								if err := check(v.Index(j)); err != nil {
									return err
								}
							}
						}
						if v.Kind() == reflect.String && isSource(v.String()) {
							return E.New(path, ".", field.Name, ": subscription ", v.String(), " is a source, not an outbound; use import")
						}
						return nil
					}
					if err := check(v); err != nil {
						return err
					}
				} else if err := validate(v, path+"."+field.Name); err != nil {
					return err
				}
			}
		case reflect.Slice:
			// RawMessage contains the complete original document. Bytes cannot
			// contain typed references; do not revisit every byte on each update.
			if value.Type().Elem().Kind() == reflect.Uint8 {
				return nil
			}
			for i := 0; i < value.Len(); i++ {
				if err := validate(value.Index(i), path+"["+strconv.Itoa(i)+"]"); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return validate(reflect.ValueOf(options), "config")
}
