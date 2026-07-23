package doctest_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/wftest"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// Types and OrderWorkflow mirror docs/03-api.md, adapted to the current
// name-based Execute / ExecuteAsync API (function-ref overloads are deferred).

type OrderInput struct{ OrderID string }
type OrderResult struct{ InvoiceID string }

type ChargeInput struct{ OrderID string }
type ChargeResult struct {
	InvoiceID     string
	CustomerEmail string
}
type ShipInput struct{ OrderID string }
type ShipResult struct{ TrackingID string }
type MailInput struct{ To string }

func OrderWorkflow(ctx *workflow.Context, in OrderInput) (OrderResult, error) {
	charge, err := workflow.Execute[ChargeInput, ChargeResult](ctx, "ChargePayment", ChargeInput{OrderID: in.OrderID},
		workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 5}))
	if err != nil {
		return OrderResult{}, err
	}

	ship := workflow.ExecuteAsync[ShipInput, ShipResult](ctx, "ShipOrder", ShipInput{OrderID: in.OrderID})
	mail := workflow.ExecuteAsync[MailInput, struct{}](ctx, "SendReceiptMail", MailInput{To: charge.CustomerEmail})
	if _, err := ship.Get(ctx); err != nil {
		return OrderResult{}, err
	}
	if _, err := mail.Get(ctx); err != nil {
		return OrderResult{}, err
	}

	if err := workflow.Sleep(ctx, 7*24*time.Hour); err != nil {
		return OrderResult{}, err
	}
	if _, err := workflow.Execute[MailInput, struct{}](ctx, "SendFollowUpMail", MailInput{To: charge.CustomerEmail}); err != nil {
		return OrderResult{}, err
	}
	return OrderResult{InvoiceID: charge.InvoiceID}, nil
}

func ChargePayment(ctx context.Context, in ChargeInput) (ChargeResult, error) {
	return ChargeResult{InvoiceID: "inv-" + in.OrderID, CustomerEmail: "buyer@example.com"}, nil
}

func ShipOrder(ctx context.Context, in ShipInput) (ShipResult, error) {
	return ShipResult{TrackingID: "t-" + in.OrderID}, nil
}

func SendReceiptMail(ctx context.Context, in MailInput) (struct{}, error) {
	return struct{}{}, nil
}

func SendFollowUpMail(ctx context.Context, in MailInput) (struct{}, error) {
	return struct{}{}, nil
}

func TestOrderWorkflow_DocExample(t *testing.T) {
	env := wftest.New(t)
	wftest.RegisterActivity(env, ChargePayment, wftest.WithName("ChargePayment"))
	wftest.RegisterActivity(env, ShipOrder, wftest.WithName("ShipOrder"))
	wftest.RegisterActivity(env, SendReceiptMail, wftest.WithName("SendReceiptMail"))
	wftest.RegisterActivity(env, SendFollowUpMail, wftest.WithName("SendFollowUpMail"))

	res, err := wftest.Run(env, OrderWorkflow, OrderInput{OrderID: "order-123"})
	if err != nil {
		t.Fatal(err)
	}
	if res.InvoiceID != "inv-order-123" {
		t.Fatalf("got %#v", res)
	}
}
