package clashapi

import (
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"
)

func getEscapeParam(r *http.Request, paramName string) string {
	param := chi.URLParam(r, paramName)
	// Chi routes on RawPath when present, otherwise on the already decoded
	// Path. Unescaping Path again would alias literal names like node%41
	// to nodeA and could target a different proxy or provider.
	if r.URL.RawPath != "" {
		if newParam, err := url.PathUnescape(param); err == nil {
			param = newParam
		}
	}
	return param
}
