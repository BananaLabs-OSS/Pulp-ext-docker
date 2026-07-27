package dockerext

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// FleetEffectCapability is the narrow host capability through which a Fleet
// state owner can settle its durable runtime effects. It intentionally does
// not expose spawn.docker: the guest gets one canonical effect operation, not
// a Docker client or an arbitrary command surface.
const FleetEffectCapability = "effect.fleet.runtime"

const fleetEffectExecuteExport = "fleet_effect_execute"

const fleetInstantExtensionAnnouncement = "Server extended! You have more time. Enjoy!"

var fleetAnnounceReasons = map[string]struct{}{
	"expiry-warning":            {},
	"scheduled-restart-warning": {},
}

func init() {
	ext.Register(newFleetEffectCapability(hostFleetEffectExecutors))
}

func newFleetEffectCapability(factory *ScopedEffectExecutorFactory) ext.Capability {
	return ext.Capability{
		Name:     FleetEffectCapability,
		Provider: "github.com/BananaLabs-OSS/Pulp-ext-docker",
		Register: func(builder wazero.HostModuleBuilder, cell ext.Cell) error {
			callerScope, err := ext.ValidatedScopeOf(cell)
			if err != nil {
				return fmt.Errorf("docker effects: bind fleet runtime scope: %w", err)
			}
			scope, err := fleetRuntimeScope(callerScope)
			if err != nil {
				return fmt.Errorf("docker effects: bind fleet runtime target scope: %w", err)
			}
			builder.NewFunctionBuilder().WithFunc(func(ctx context.Context, module api.Module, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
				return fleetEffectExecute(ctx, module, factory, scope, requestPtr, requestLen, responsePtrPtr, responseLenPtr)
			}).Export(fleetEffectExecuteExport)
			return nil
		},
		Stub: func(builder wazero.HostModuleBuilder, _ ext.Cell) error {
			builder.NewFunctionBuilder().WithFunc(fleetEffectExecuteStub).Export(fleetEffectExecuteExport)
			return nil
		},
		TeardownScope: func(ctx context.Context, scope ext.Scope) error {
			return releaseHostScope(ctx, scope, factory)
		},
	}
}

// ReleaseHostScope idempotently releases every Pulp-ext-docker resource owned
// by exactly one application instance: the Fleet executor and replay cache,
// scoped Docker routing/build/event state, event subscription, and provider
// transport. It requires a validated scope and never exposes a global reset.
//
// A deployment host should call this before replacing an application host so
// a subsequent instance cannot inherit a provider created from an earlier
// DOCKER_HOST.
func ReleaseHostScope(ctx context.Context, scope ext.Scope) error {
	return releaseHostScope(ctx, scope, hostFleetEffectExecutors)
}

func releaseHostScope(ctx context.Context, scope ext.Scope, factory *ScopedEffectExecutorFactory) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("docker effects: release host scope: %w", err)
	}
	if factory != nil {
		if err := factory.TeardownScope(scope); err != nil {
			return fmt.Errorf("docker effects: release executor scope: %w", err)
		}
	}
	if err := teardownDockerScope(ctx, scope); err != nil {
		return fmt.Errorf("docker effects: release Docker scope: %w", err)
	}
	return nil
}

// fleet_effect_execute has the stable guest ABI:
//
//	fleet_effect_execute(request_ptr, request_len, response_ptr_ptr, response_len_ptr) -> u32
//
// request is MessagePack effect.Intent and response is MessagePack
// effect.Receipt. The captured Fleet scope is host-owned; guest bytes never
// select an application, cell, or runtime instance.
func fleetEffectExecute(ctx context.Context, module api.Module, factory *ScopedEffectExecutorFactory, scope ext.Scope, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
	if module == nil || module.Memory() == nil {
		return codeMemoryRead
	}
	request, ok := module.Memory().Read(requestPtr, requestLen)
	if !ok {
		return codeMemoryRead
	}
	response, code := executeFleetEffectWire(ctx, factory, scope, request)
	if code != codeOK {
		return code
	}
	return writeRawResponse(ctx, module, response, responsePtrPtr, responseLenPtr)
}

func fleetEffectExecuteStub(_ context.Context, _ api.Module, _, _, _, _ uint32) uint32 {
	return codeCapabilityStubbed
}

// executeFleetEffectWire isolates the canonical MessagePack boundary from
// guest-memory plumbing. It is deliberately closed over effect.Intent and
// effect.Receipt so callers cannot smuggle a legacy Docker operation through
// the host capability.
func executeFleetEffectWire(ctx context.Context, factory *ScopedEffectExecutorFactory, scope ext.Scope, request []byte) ([]byte, uint32) {
	if factory == nil {
		return nil, codeProviderUnavail
	}
	intent, err := effect.UnmarshalIntent(request)
	if err != nil {
		if !strings.Contains(err.Error(), "decode effect intent:") {
			return nil, codeInvalidRequest
		}
		return nil, codeMsgpackDecode
	}
	if err := validateFleetRuntimeIntent(intent); err != nil {
		return nil, codeInvalidRequest
	}
	executor, err := factory.ForScope(scope)
	if err != nil {
		return nil, fleetEffectErrorCode(err)
	}
	receipt, err := executor.ExecuteIntent(ctx, scope, intent)
	if err != nil {
		return nil, fleetEffectErrorCode(err)
	}
	response, err := effect.MarshalReceipt(receipt)
	if err != nil {
		return nil, codeMsgpackEncode
	}
	return response, codeOK
}

// fleetRuntimeScope gives an effects/primary guest access to precisely the
// same application-local fleet/primary executor used by the deployment's
// outbox dispatcher. It never accepts a scope supplied by guest bytes, and it
// cannot cross application or application-instance boundaries.
func fleetRuntimeScope(caller ext.Scope) (ext.Scope, error) {
	if err := caller.Validate(); err != nil {
		return ext.Scope{}, err
	}
	return ext.NewScope(caller.ApplicationID(), caller.ApplicationInstanceID(), "fleet", "primary")
}

// validateFleetRuntimeIntent is intentionally much narrower than the Docker
// executor. The guest ABI exists only for the two synchronous Fleet operations
// the application needs today: retry-safe server deprovision, save_flush, and
// the audited announcement envelopes used by extension and warning workflows.
// Provisioning and every other Docker/RCON operation continue to flow through
// their typed owner/outbox boundaries.
func validateFleetRuntimeIntent(intent effect.Intent) error {
	switch intent.Kind {
	case effect.KindFleetServerDeprovision:
		return nil
	case effect.KindFleetExtensionApply:
		payload, err := effect.DecodePayload[map[string]any](intent)
		if err != nil {
			return err
		}
		decoded, err := decodeFleetExtensionPayload(payload)
		if err != nil {
			return err
		}
		if decoded.Extension != "rcon" {
			if decoded.Extension == "restart" || decoded.Extension == "regenerate" {
				return validateFleetRuntimeOperation(payload, decoded)
			}
			return errors.New("docker effects: fleet runtime extension is not permitted")
		}
		if decoded.RCONAction == "save_flush" {
			return nil
		}
		if decoded.RCONAction == "announce" {
			return validateFleetRuntimeAnnounce(payload, decoded)
		}
		return errors.New("docker effects: fleet runtime only permits rcon save_flush and audited announce")
	default:
		return fmt.Errorf("docker effects: fleet runtime does not permit effect %q", intent.Kind)
	}
}

func validateFleetRuntimeOperation(payload map[string]any, decoded fleetExtensionPayload) error {
	allowed := map[string]struct{}{
		"extension": {}, "server_id": {}, "node_id": {}, "container_id": {}, "reason": {},
	}
	for field := range payload {
		if _, ok := allowed[field]; !ok {
			return fmt.Errorf("docker effects: fleet runtime %s payload field %q is not permitted", decoded.Extension, field)
		}
	}
	for _, field := range []string{"extension", "server_id", "node_id", "container_id"} {
		if _, ok := payload[field]; !ok {
			return fmt.Errorf("docker effects: fleet runtime %s payload field %q is required", decoded.Extension, field)
		}
	}
	for name, value := range map[string]string{
		"server_id": decoded.ServerID, "node_id": decoded.NodeID, "container_id": decoded.ContainerID,
	} {
		if !validFleetRuntimeField(value) {
			return fmt.Errorf("docker effects: fleet runtime %s %s is required", decoded.Extension, name)
		}
	}
	if _, present := payload["reason"]; present && decoded.Reason != "" && !validFleetRuntimeField(decoded.Reason) {
		return fmt.Errorf("docker effects: fleet runtime %s reason is invalid", decoded.Extension)
	}
	return nil
}

func validFleetRuntimeField(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 256 {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validateFleetRuntimeAnnounce(payload map[string]any, decoded fleetExtensionPayload) error {
	allowed := map[string]struct{}{
		"extension": {}, "server_id": {}, "node_id": {}, "container_id": {},
		"rcon_action": {}, "message": {}, "reason": {},
	}
	for field := range payload {
		if _, ok := allowed[field]; !ok {
			return fmt.Errorf("docker effects: fleet runtime announce payload field %q is not permitted", field)
		}
	}
	for _, field := range []string{"extension", "server_id", "node_id", "container_id", "rcon_action", "message"} {
		if _, ok := payload[field]; !ok {
			return fmt.Errorf("docker effects: fleet runtime announce payload field %q is required", field)
		}
	}
	if strings.TrimSpace(decoded.ServerID) == "" || strings.TrimSpace(decoded.NodeID) == "" || strings.TrimSpace(decoded.ContainerID) == "" {
		return errors.New("docker effects: fleet runtime announce requires exact server, node, and container identity")
	}
	if !safeRCONText(decoded.Message, 500) {
		return errors.New("docker effects: RCON announcement is invalid")
	}
	if decoded.Reason == "" {
		if decoded.Message != fleetInstantExtensionAnnouncement {
			return errors.New("docker effects: fleet runtime announce reason is required")
		}
		return nil
	}
	if _, ok := fleetAnnounceReasons[decoded.Reason]; !ok {
		return fmt.Errorf("docker effects: fleet runtime announce reason %q is not permitted", decoded.Reason)
	}
	return nil
}

// fleetEffectErrorCode deliberately retains the established Docker ABI codes:
// malformed/unsupported durable intents are request errors (1), unavailable
// Docker runtime is 10, a missing target is 6, and all other privileged host
// failures are 4. Deprovision's already-gone target is converted to a
// completed receipt by hostFleetBackend before this mapping is reached.
func fleetEffectErrorCode(err error) uint32 {
	if err == nil {
		return codeOK
	}
	if isNotFound(err) {
		return codeNotFound
	}
	message := err.Error()
	if strings.Contains(message, "host fleet runtime unavailable") || strings.Contains(message, "executor is not configured") {
		return codeProviderUnavail
	}
	if isFleetEffectRequestError(message) {
		return codeInvalidRequest
	}
	return codeDockerError
}

func isFleetEffectRequestError(message string) bool {
	for _, marker := range []string{
		"normalize intent:",
		"decode intent payload:",
		"unsupported fleet effect kind",
		"idempotency key ",
		"payload \"",
		"fleet extension ",
		"RCON action ",
		"RCON announcement is invalid",
		"RCON gamerule is invalid",
		"RCON difficulty is invalid",
		"RCON default game mode is invalid",
		"approved provision image envelope is invalid",
		"provision image is required",
		"provision volumes:",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}
