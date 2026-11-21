package clashapi

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

func TestControlPathParametersDecodedOnce(t *testing.T) {
	for _, name := range []string{"香港", "node%41", "node%2F", "100%", "東京 / 100%+?#", "node%2541"} {
		t.Run(name, func(t *testing.T) {
			for _, provider := range []string{"source", "机场 / A"} {
				router := chi.NewRouter()
				router.Route("/providers/proxies", func(r chi.Router) {
					r.Get("/{name}/{proxy}/healthcheck", func(w http.ResponseWriter, r *http.Request) {
						require.Equal(t, provider, getEscapeParam(r, "name"))
						require.Equal(t, name, getEscapeParam(r, "proxy"))
						w.WriteHeader(http.StatusNoContent)
					})
				})
				response := httptest.NewRecorder()
				path := "/providers/proxies/" + url.PathEscape(provider) + "/" + url.PathEscape(name) + "/healthcheck"
				router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
				require.Equal(t, http.StatusNoContent, response.Code)
			}
		})
	}
}
