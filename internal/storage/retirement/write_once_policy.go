package retirement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"golang.org/x/xerrors"
)

const cscbWriteOncePolicyMode = "bucket_deny_unconditional_put_delete_replication_and_lifecycle_mutation_no_matching_lifecycle_actions"

type (
	policyStringList []string

	bucketPolicyDocument struct {
		Statements bucketPolicyStatements `json:"Statement"`
	}

	bucketPolicyStatements []bucketPolicyStatement

	bucketPolicyStatement struct {
		Effect       string                                 `json:"Effect"`
		Principal    json.RawMessage                        `json:"Principal"`
		NotPrincipal json.RawMessage                        `json:"NotPrincipal"`
		Action       policyStringList                       `json:"Action"`
		NotAction    policyStringList                       `json:"NotAction"`
		Resource     policyStringList                       `json:"Resource"`
		NotResource  policyStringList                       `json:"NotResource"`
		Condition    map[string]map[string]policyStringList `json:"Condition"`
	}
)

func (s *S3ObjectStore) InspectObjectRetentionSafety(ctx context.Context, bucket string, key string) (RetentionSafetySnapshot, error) {
	if s == nil || s.client == nil {
		return RetentionSafetySnapshot{}, xerrors.New("s3 client is required to verify the CSCB write-once policy")
	}
	if bucket == "" || key == "" || !strings.Contains(key, "/consolidated/") {
		return RetentionSafetySnapshot{}, xerrors.Errorf("valid CSCB bucket and consolidated object key are required: bucket=%q key=%q", bucket, key)
	}
	out, err := s.client.GetBucketPolicy(ctx, &awss3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		return RetentionSafetySnapshot{}, xerrors.Errorf("failed to get CSCB bucket write-once policy (bucket=%s): %w", bucket, err)
	}
	if out == nil || out.Policy == nil || strings.TrimSpace(aws.ToString(out.Policy)) == "" {
		return RetentionSafetySnapshot{}, xerrors.Errorf("CSCB bucket has no readable write-once policy: bucket=%s", bucket)
	}
	policy, err := decodeBucketPolicy(aws.ToString(out.Policy))
	if err != nil {
		return RetentionSafetySnapshot{}, xerrors.Errorf("failed to decode CSCB bucket policy (bucket=%s): %w", bucket, err)
	}
	lifecycleRules, lifecycleErr := s.getLifecycleRules(ctx, bucket)
	snapshot, snapshotErr := retentionSafetySnapshot(policy, lifecycleRules)
	if snapshotErr != nil {
		return RetentionSafetySnapshot{}, snapshotErr
	}
	if lifecycleErr != nil {
		return snapshot, lifecycleErr
	}
	objectARN := "arn:aws:s3:::" + bucket + "/" + key
	bucketARN := "arn:aws:s3:::" + bucket
	writeProtected := false
	mutationProtected := false
	lifecycleConfigurationProtected := false
	for _, statement := range policy.Statements {
		if statementEnforcesCSCBWriteOnce(statement, objectARN) {
			writeProtected = true
		}
		if statementEnforcesCSCBMutationProtection(statement, objectARN) {
			mutationProtected = true
		}
		if statementEnforcesLifecycleConfigurationProtection(statement, bucketARN) {
			lifecycleConfigurationProtected = true
		}
	}
	if !writeProtected || !mutationProtected || !lifecycleConfigurationProtected {
		return snapshot, xerrors.Errorf(
			"bucket policy does not make CSCB object retention-safe (conditional_put=%t mutation_protected=%t lifecycle_configuration_protected=%t): bucket=%s key=%s",
			writeProtected,
			mutationProtected,
			lifecycleConfigurationProtected,
			bucket,
			key,
		)
	}
	if err := verifyNoMatchingLifecycleActions(bucket, key, lifecycleRules); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

func (s *S3ObjectStore) getLifecycleRules(ctx context.Context, bucket string) ([]awss3types.LifecycleRule, error) {
	out, err := s.client.GetBucketLifecycleConfiguration(
		ctx,
		&awss3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(bucket)},
	)
	if err != nil {
		if isNoSuchLifecycleConfiguration(err) {
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get CSCB bucket lifecycle configuration (bucket=%s): %w", bucket, err)
	}
	if out == nil {
		return nil, xerrors.Errorf("CSCB bucket lifecycle configuration returned an empty response: bucket=%s", bucket)
	}
	return out.Rules, nil
}

func verifyNoMatchingLifecycleActions(bucket string, key string, rules []awss3types.LifecycleRule) error {
	for _, rule := range rules {
		if rule.Status != awss3types.ExpirationStatusEnabled || !lifecycleRuleHasObjectDataAction(rule) || !lifecycleRuleMayMatchKey(rule, key) {
			continue
		}
		return xerrors.Errorf(
			"enabled S3 lifecycle rule can expire or transition CSCB object data: bucket=%s key=%s rule_id=%q",
			bucket,
			key,
			aws.ToString(rule.ID),
		)
	}
	return nil
}

func retentionSafetySnapshot(policy bucketPolicyDocument, rules []awss3types.LifecycleRule) (RetentionSafetySnapshot, error) {
	encoded, err := json.Marshal(struct {
		Policy bucketPolicyDocument       `json:"policy"`
		Rules  []awss3types.LifecycleRule `json:"lifecycle_rules"`
	}{
		Policy: policy,
		Rules:  rules,
	})
	if err != nil {
		return RetentionSafetySnapshot{}, xerrors.Errorf("failed to encode CSCB retention safety configuration: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return RetentionSafetySnapshot{ConfigurationSHA256: hex.EncodeToString(digest[:])}, nil
}

func lifecycleRuleHasObjectDataAction(rule awss3types.LifecycleRule) bool {
	return rule.Expiration != nil ||
		rule.NoncurrentVersionExpiration != nil ||
		len(rule.Transitions) > 0 ||
		len(rule.NoncurrentVersionTransitions) > 0
}

func lifecycleRuleMayMatchKey(rule awss3types.LifecycleRule, key string) bool {
	prefix, hasPrefix := lifecycleRulePrefix(rule)
	return !hasPrefix || strings.HasPrefix(key, prefix)
}

func lifecycleRulePrefix(rule awss3types.LifecycleRule) (string, bool) {
	if rule.Filter != nil {
		if rule.Filter.And != nil && rule.Filter.And.Prefix != nil {
			return aws.ToString(rule.Filter.And.Prefix), true
		}
		if rule.Filter.Prefix != nil {
			return aws.ToString(rule.Filter.Prefix), true
		}
		return "", false
	}
	if rule.Prefix != nil {
		return aws.ToString(rule.Prefix), true
	}
	return "", false
}

func isNoSuchLifecycleConfiguration(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchLifecycleConfiguration"
}

func decodeBucketPolicy(value string) (bucketPolicyDocument, error) {
	var policy bucketPolicyDocument
	if err := json.Unmarshal([]byte(value), &policy); err == nil {
		return policy, nil
	}
	decoded, err := url.QueryUnescape(value)
	if err != nil {
		return bucketPolicyDocument{}, err
	}
	if err := json.Unmarshal([]byte(decoded), &policy); err != nil {
		return bucketPolicyDocument{}, err
	}
	return policy, nil
}

func statementEnforcesCSCBWriteOnce(statement bucketPolicyStatement, objectARN string) bool {
	if statement.Effect != "Deny" || len(statement.NotPrincipal) != 0 || len(statement.NotAction) != 0 || len(statement.NotResource) != 0 {
		return false
	}
	if !principalIncludesEveryone(statement.Principal) || !policyPatternsInclude(statement.Action, "s3:PutObject") ||
		!policyPatternsInclude(statement.Resource, objectARN) {
		return false
	}
	if len(statement.Condition) < 1 || len(statement.Condition) > 2 {
		return false
	}
	nullConditions, ok := statement.Condition["Null"]
	if !ok || len(nullConditions) != 1 {
		return false
	}
	values, ok := nullConditions["s3:if-none-match"]
	if !ok || len(values) != 1 || values[0] != "true" {
		return false
	}
	if len(statement.Condition) == 1 {
		return true
	}
	boolConditions, ok := statement.Condition["Bool"]
	if !ok || len(boolConditions) != 1 {
		return false
	}
	values, ok = boolConditions["s3:ObjectCreationOperation"]
	return ok && len(values) == 1 && values[0] == "true"
}

func statementEnforcesCSCBMutationProtection(statement bucketPolicyStatement, objectARN string) bool {
	if statement.Effect != "Deny" || len(statement.NotPrincipal) != 0 || len(statement.NotAction) != 0 || len(statement.NotResource) != 0 || len(statement.Condition) != 0 {
		return false
	}
	return principalIncludesEveryone(statement.Principal) &&
		policyPatternsInclude(statement.Action, "s3:DeleteObject") &&
		policyPatternsInclude(statement.Action, "s3:DeleteObjectVersion") &&
		policyPatternsInclude(statement.Action, "s3:ReplicateObject") &&
		policyPatternsInclude(statement.Action, "s3:ReplicateDelete") &&
		policyPatternsInclude(statement.Resource, objectARN)
}

func statementEnforcesLifecycleConfigurationProtection(statement bucketPolicyStatement, bucketARN string) bool {
	if statement.Effect != "Deny" || len(statement.NotPrincipal) != 0 || len(statement.NotAction) != 0 || len(statement.NotResource) != 0 || len(statement.Condition) != 0 {
		return false
	}
	return principalIncludesEveryone(statement.Principal) &&
		policyPatternsInclude(statement.Action, "s3:PutLifecycleConfiguration") &&
		policyPatternsInclude(statement.Resource, bucketARN)
}

func principalIncludesEveryone(value json.RawMessage) bool {
	if len(value) == 0 {
		return false
	}
	var principal string
	if err := json.Unmarshal(value, &principal); err == nil {
		return principal == "*"
	}
	var principals map[string]policyStringList
	if err := json.Unmarshal(value, &principals); err != nil {
		return false
	}
	for _, key := range []string{"AWS", "*"} {
		for _, candidate := range principals[key] {
			if candidate == "*" {
				return true
			}
		}
	}
	return false
}

func policyPatternsInclude(patterns policyStringList, value string) bool {
	for _, pattern := range patterns {
		if policyPatternMatches(pattern, value) {
			return true
		}
	}
	return false
}

func policyPatternMatches(pattern string, value string) bool {
	expression := regexp.QuoteMeta(pattern)
	expression = strings.ReplaceAll(expression, `\*`, `.*`)
	expression = strings.ReplaceAll(expression, `\?`, `.`)
	matched, err := regexp.MatchString("^"+expression+"$", value)
	return err == nil && matched
}

func (values *policyStringList) UnmarshalJSON(data []byte) error {
	var one string
	if err := json.Unmarshal(data, &one); err == nil {
		*values = []string{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return err
	}
	*values = many
	return nil
}

func (statements *bucketPolicyStatements) UnmarshalJSON(data []byte) error {
	var many []bucketPolicyStatement
	if err := json.Unmarshal(data, &many); err == nil {
		*statements = many
		return nil
	}
	var one bucketPolicyStatement
	if err := json.Unmarshal(data, &one); err != nil {
		return err
	}
	*statements = []bucketPolicyStatement{one}
	return nil
}
