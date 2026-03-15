package store

import (
	"testing"

	"github.com/macho_prawn/events-dashboard/internal/models"
)

func TestReplayProtectionColumnForSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		schema models.TableSchema
		want   string
	}{
		{
			name:   "news article id",
			source: "News",
			schema: models.TableSchema{{Name: "article_id", Type: "text", Required: true}},
			want:   "article_id",
		},
		{
			name:   "flights flight id",
			source: "Flights",
			schema: models.TableSchema{{Name: "flight_id", Type: "text", Required: true}},
			want:   "flight_id",
		},
		{
			name:   "events invoice number",
			source: "Events",
			schema: models.TableSchema{{Name: "invoice_number", Type: "text", Required: true}},
			want:   "invoice_number",
		},
		{
			name:   "events event id fallback",
			source: "Events",
			schema: models.TableSchema{{Name: "event_id", Type: "text", Required: true}},
			want:   "event_id",
		},
		{
			name:   "ecommerce order id",
			source: "ECommerce",
			schema: models.TableSchema{{Name: "order_id", Type: "text", Required: true}},
			want:   "order_id",
		},
		{
			name:   "ecommerce lowercase source canonicalized",
			source: "ecommerce",
			schema: models.TableSchema{{Name: "order_id", Type: "text", Required: true}},
			want:   "order_id",
		},
		{
			name:   "ecommerce transaction id fallback",
			source: "ECommerce",
			schema: models.TableSchema{{Name: "transaction_id", Type: "text", Required: true}},
			want:   "transaction_id",
		},
		{
			name:   "news without article id",
			source: "News",
			schema: models.TableSchema{{Name: "headline", Type: "text", Required: true}},
			want:   "",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := replayProtectionColumnForSource(test.source, test.schema)
			if got != test.want {
				t.Fatalf("replayProtectionColumnForSource(%q) = %q, want %q", test.source, got, test.want)
			}
		})
	}
}
