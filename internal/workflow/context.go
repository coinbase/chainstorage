package workflow

import "context"

type workflowIDContextKey struct{}

// WithWorkflowID overrides the default Temporal workflow ID for manual/admin starts.
func WithWorkflowID(ctx context.Context, workflowID string) context.Context {
	if workflowID == "" {
		return ctx
	}
	return context.WithValue(ctx, workflowIDContextKey{}, workflowID)
}

func workflowIDFromContext(ctx context.Context) (string, bool) {
	workflowID, ok := ctx.Value(workflowIDContextKey{}).(string)
	return workflowID, ok && workflowID != ""
}
