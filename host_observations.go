package dockerext

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	fleetObservationMaxBytes      = 64 << 10
	fleetObservationMaxPlayers    = 256
	fleetObservationMaxIdentities = 512
	fleetObservationMaxArtifacts  = 256
)

const (
	fleetObservationSettingsCommand      = "cat /data/server.properties 2>/dev/null || echo ''"
	fleetObservationPlayerHistoryCommand = "(test -f /data/usercache.json && cat /data/usercache.json) || echo '[]'"
	fleetObservationWhitelistCommand     = "(test -f /data/whitelist.json && cat /data/whitelist.json) || echo '[]'"
	fleetObservationOpsCommand           = "(test -f /data/ops.json && cat /data/ops.json) || echo '[]'"
	fleetObservationBansCommand          = "(test -f /data/banned-players.json && cat /data/banned-players.json) || echo '[]'"
	fleetObservationDatapacksCommand     = "ls -1p /data/world/datapacks/ 2>/dev/null || echo ''"
	fleetObservationModsCommand          = "ls -1p /data/mods/ 2>/dev/null || echo ''"
)

var fleetObservationSettings = map[string]string{
	"difficulty": "difficulty", "gamemode": "gamemode", "pvp": "pvp", "hardcore": "hardcore",
	"allow-nether": "allow_nether", "spawn-monsters": "spawn_monsters", "spawn-animals": "spawn_animals",
	"view-distance": "view_distance", "simulation-distance": "simulation_distance", "motd": "motd",
}

var fleetObservationGameRules = []string{
	"announceAdvancements", "commandBlockOutput", "doDaylightCycle", "doImmediateRespawn", "doMobSpawning",
	"doWeatherCycle", "doInsomnia", "drowningDamage", "fallDamage", "fireDamage", "forgiveDeadPlayers", "keepInventory",
	"mobGriefing", "naturalRegeneration", "playersSleepingPercentage", "randomTickSpeed", "showDeathMessages",
	"spawnRadius", "universalAnger",
}

func (b *hostFleetBackend) executeRuntimeObservation(ctx context.Context, request CanonicalFleetRequest) (map[string]any, error) {
	payload, err := decodeRuntimeObservationPayload(request.Payload)
	if err != nil {
		return nil, err
	}
	runtime, err := b.runtime(ctx)
	if err != nil {
		return nil, err
	}
	server, err := runtime.Get(ctx, payload.ContainerID)
	if err != nil {
		return nil, err
	}
	expectedName := scopePrefix(b.scope) + payload.ServerID
	if server == nil || server.ID != payload.ContainerID || strings.TrimPrefix(server.Name, "/") != expectedName || !nameOwnedByScope(server.Name, b.scope) {
		return nil, errors.New("docker effects: runtime observation target is outside the exact scoped server identity")
	}

	observedAt := time.Now().UTC().Format(time.RFC3339Nano)
	receipt := effect.FleetRuntimeObservationReceiptV1{
		Contract: payload.Contract, ServerID: payload.ServerID, NodeID: payload.NodeID,
		ContainerID: payload.ContainerID, Field: payload.Field, Generation: payload.Generation,
		SourceRevision: payload.SourceRevision, ObservedAt: observedAt,
	}
	if err := populateRuntimeObservation(ctx, runtime, payload, observedAt, &receipt); err != nil {
		return nil, err
	}
	if err := receipt.ValidateFor(payload); err != nil {
		return nil, fmt.Errorf("docker effects: validate runtime observation receipt: %w", err)
	}
	wire, err := msgpack.Marshal(receipt)
	if err != nil {
		return nil, fmt.Errorf("docker effects: encode runtime observation receipt: %w", err)
	}
	var result map[string]any
	if err := msgpack.Unmarshal(wire, &result); err != nil {
		return nil, fmt.Errorf("docker effects: project runtime observation receipt: %w", err)
	}
	return result, nil
}

func decodeRuntimeObservationPayload(payload map[string]any) (effect.FleetRuntimeObservationIntentV1, error) {
	wire, err := msgpack.Marshal(payload)
	if err != nil {
		return effect.FleetRuntimeObservationIntentV1{}, fmt.Errorf("docker effects: encode runtime observation intent: %w", err)
	}
	var value effect.FleetRuntimeObservationIntentV1
	decoder := msgpack.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields(true)
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("docker effects: decode runtime observation intent: %w", err)
	}
	if err := value.Validate(); err != nil {
		return value, fmt.Errorf("docker effects: validate runtime observation intent: %w", err)
	}
	return value, nil
}

func populateRuntimeObservation(ctx context.Context, runtime HostFleetRuntime, payload effect.FleetRuntimeObservationIntentV1, observedAt string, receipt *effect.FleetRuntimeObservationReceiptV1) error {
	if payload.Field == effect.FleetRuntimeObservationFieldStatusV1 {
		server, err := runtime.Get(ctx, payload.ContainerID)
		if err != nil {
			return err
		}
		receipt.Data.Status = string(server.Status)
		return nil
	}
	execRuntime, ok := runtime.(HostFleetExecRuntime)
	if !ok {
		return errors.New("docker effects: scoped runtime does not support bounded observation exec")
	}
	exec := func(command []string) (string, error) {
		output, err := execRuntime.Exec(ctx, payload.ContainerID, command)
		if err != nil {
			return "", err
		}
		if len(output) > fleetObservationMaxBytes {
			return "", errors.New("docker effects: runtime observation output exceeds limit")
		}
		return output, nil
	}

	switch payload.Field {
	case effect.FleetRuntimeObservationFieldSettingsV1:
		output, err := exec([]string{"sh", "-c", fleetObservationSettingsCommand})
		if err != nil {
			return err
		}
		receipt.Data.Settings, err = parseRuntimeObservationSettings(output)
		return err
	case effect.FleetRuntimeObservationFieldGameRulesV1:
		output, err := exec([]string{"sh", "-c", fleetObservationGameRulesCommand()})
		if err != nil {
			return err
		}
		receipt.Data.GameRules, err = parseRuntimeObservationGameRules(output)
		return err
	case effect.FleetRuntimeObservationFieldPlayersV1:
		output, err := exec([]string{"rcon", "list"})
		if err != nil {
			return err
		}
		receipt.Data.Players, err = parseRuntimeObservationPlayers(output)
		return err
	case effect.FleetRuntimeObservationFieldPlayerHistoryV1:
		output, err := exec([]string{"sh", "-c", fleetObservationPlayerHistoryCommand})
		if err != nil {
			return err
		}
		receipt.Data.PlayerHistory, err = parseRuntimeObservationPlayerHistory(output)
		return err
	case effect.FleetRuntimeObservationFieldAccessV1:
		whitelist, err := exec([]string{"sh", "-c", fleetObservationWhitelistCommand})
		if err != nil {
			return err
		}
		operators, err := exec([]string{"sh", "-c", fleetObservationOpsCommand})
		if err != nil {
			return err
		}
		bans, err := exec([]string{"sh", "-c", fleetObservationBansCommand})
		if err != nil {
			return err
		}
		access := &effect.FleetRuntimeObservationAccessV1{ServerID: payload.ServerID, UpdatedAt: observedAt}
		if access.Whitelist, err = parseRuntimeObservationIdentities(whitelist); err != nil {
			return err
		}
		if access.Operators, err = parseRuntimeObservationIdentities(operators); err != nil {
			return err
		}
		if access.Bans, err = parseRuntimeObservationIdentities(bans); err != nil {
			return err
		}
		receipt.Data.Access = access
		return nil
	case effect.FleetRuntimeObservationFieldAccessSnapshotV1:
		whitelist, err := exec([]string{"sh", "-c", fleetObservationWhitelistCommand})
		if err != nil {
			return err
		}
		operators, err := exec([]string{"sh", "-c", fleetObservationOpsCommand})
		if err != nil {
			return err
		}
		bans, err := exec([]string{"sh", "-c", fleetObservationBansCommand})
		if err != nil {
			return err
		}
		snapshot := &effect.FleetRuntimeObservationAccessSnapshotV1{ServerID: payload.ServerID, UpdatedAt: observedAt}
		if snapshot.Whitelist, err = parseRuntimeObservationWhitelistSnapshot(whitelist); err != nil {
			return err
		}
		if snapshot.Operators, err = parseRuntimeObservationOperatorSnapshot(operators); err != nil {
			return err
		}
		if snapshot.Bans, err = parseRuntimeObservationBanSnapshot(bans); err != nil {
			return err
		}
		receipt.Data.AccessSnapshot = snapshot
		return nil
	case effect.FleetRuntimeObservationFieldArtifactsV1:
		datapacks, err := exec([]string{"sh", "-c", fleetObservationDatapacksCommand})
		if err != nil {
			return err
		}
		mods, err := exec([]string{"sh", "-c", fleetObservationModsCommand})
		if err != nil {
			return err
		}
		receipt.Data.Artifacts, err = parseRuntimeObservationArtifacts(datapacks, mods)
		return err
	default:
		return fmt.Errorf("docker effects: runtime observation field %q is not configured", payload.Field)
	}
}

func parseRuntimeObservationSettings(output string) (map[string]string, error) {
	result := make(map[string]string)
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		responseKey, ok := fleetObservationSettings[strings.TrimSpace(key)]
		if !ok {
			if strings.TrimSpace(key) != "white-list" {
				continue
			}
			responseKey = "white_list"
		}
		value = strings.TrimSpace(value)
		if len(value) > 256 || strings.IndexFunc(value, unicode.IsControl) >= 0 {
			return nil, errors.New("docker effects: invalid observed setting")
		}
		result[responseKey] = value
	}
	return result, nil
}

func fleetObservationGameRulesCommand() string {
	var command strings.Builder
	for _, rule := range fleetObservationGameRules {
		fmt.Fprintf(&command, "echo \"RULE:%s:$(rcon 'gamerule %s' 2>/dev/null)\"\n", rule, rule)
	}
	return command.String()
}

func parseRuntimeObservationGameRules(output string) (map[string]string, error) {
	allowed := make(map[string]struct{}, len(fleetObservationGameRules))
	for _, rule := range fleetObservationGameRules {
		allowed[rule] = struct{}{}
	}
	result := make(map[string]string)
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, "RULE:") {
			continue
		}
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		if _, ok := allowed[parts[1]]; !ok {
			return nil, errors.New("docker effects: unexpected observed gamerule")
		}
		fields := strings.Fields(parts[2])
		if len(fields) == 0 {
			continue
		}
		value := strings.Trim(fields[len(fields)-1], "[](){}.,:;\"'")
		if !fleetRCONName.MatchString(value) {
			return nil, errors.New("docker effects: invalid observed gamerule value")
		}
		result[parts[1]] = value
	}
	return result, nil
}

func parseRuntimeObservationPlayers(output string) ([]string, error) {
	index := strings.LastIndex(output, ":")
	if index < 0 || strings.TrimSpace(output[index+1:]) == "" {
		return []string{}, nil
	}
	parts := strings.Split(strings.TrimSpace(output[index+1:]), ",")
	if len(parts) > fleetObservationMaxPlayers {
		return nil, errors.New("docker effects: too many observed players")
	}
	players := make([]string, 0, len(parts))
	for _, item := range parts {
		name := strings.TrimSpace(item)
		if !fleetRCONName.MatchString(name) {
			return nil, errors.New("docker effects: invalid observed player")
		}
		players = append(players, name)
	}
	sort.Strings(players)
	return players, nil
}

func parseRuntimeObservationPlayerHistory(output string) ([]effect.FleetRuntimePlayerHistoryEntryV1, error) {
	var source []struct {
		UUID      string `json:"uuid"`
		Name      string `json:"name"`
		ExpiresOn string `json:"expiresOn"`
	}
	if err := json.Unmarshal([]byte(output), &source); err != nil {
		return nil, errors.New("docker effects: invalid observed player history")
	}
	if len(source) > 1000 {
		return nil, errors.New("docker effects: observed player history exceeds limit")
	}
	result := make([]effect.FleetRuntimePlayerHistoryEntryV1, 0, len(source))
	for _, item := range source {
		result = append(result, effect.FleetRuntimePlayerHistoryEntryV1{
			UUID: strings.TrimSpace(item.UUID), Name: strings.TrimSpace(item.Name), ExpiresOn: strings.TrimSpace(item.ExpiresOn),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].UUID < result[j].UUID })
	return result, nil
}

func parseRuntimeObservationIdentities(output string) ([]string, error) {
	var source []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(output), &source); err != nil {
		return nil, errors.New("docker effects: invalid observed access list")
	}
	if len(source) > fleetObservationMaxIdentities {
		return nil, errors.New("docker effects: too many observed access identities")
	}
	result := make([]string, 0, len(source))
	for _, identity := range source {
		identity.Name = strings.TrimSpace(identity.Name)
		if !fleetRCONName.MatchString(identity.Name) {
			return nil, errors.New("docker effects: invalid observed access identity")
		}
		result = append(result, identity.Name)
	}
	sort.Strings(result)
	return result, nil
}

func parseRuntimeObservationWhitelistSnapshot(output string) ([]effect.FleetRuntimeAccessSnapshotIdentityV1, error) {
	var source []struct {
		UUID string `json:"uuid"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(output), &source); err != nil {
		return nil, errors.New("docker effects: invalid observed whitelist snapshot")
	}
	if len(source) > 1000 {
		return nil, errors.New("docker effects: observed whitelist snapshot exceeds limit")
	}
	result := make([]effect.FleetRuntimeAccessSnapshotIdentityV1, 0, len(source))
	for _, item := range source {
		result = append(result, effect.FleetRuntimeAccessSnapshotIdentityV1{UUID: strings.TrimSpace(item.UUID), Name: strings.TrimSpace(item.Name)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].UUID < result[j].UUID })
	return result, nil
}

func parseRuntimeObservationOperatorSnapshot(output string) ([]effect.FleetRuntimeAccessSnapshotOperatorV1, error) {
	var source []struct {
		UUID                string `json:"uuid"`
		Name                string `json:"name"`
		Level               int    `json:"level"`
		BypassesPlayerLimit bool   `json:"bypassesPlayerLimit"`
	}
	if err := json.Unmarshal([]byte(output), &source); err != nil {
		return nil, errors.New("docker effects: invalid observed operator snapshot")
	}
	if len(source) > 1000 {
		return nil, errors.New("docker effects: observed operator snapshot exceeds limit")
	}
	result := make([]effect.FleetRuntimeAccessSnapshotOperatorV1, 0, len(source))
	for _, item := range source {
		result = append(result, effect.FleetRuntimeAccessSnapshotOperatorV1{
			UUID: strings.TrimSpace(item.UUID), Name: strings.TrimSpace(item.Name), Level: item.Level, BypassesPlayerLimit: item.BypassesPlayerLimit,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].UUID < result[j].UUID })
	return result, nil
}

func parseRuntimeObservationBanSnapshot(output string) ([]effect.FleetRuntimeAccessSnapshotBanV1, error) {
	var source []struct {
		UUID    string `json:"uuid"`
		Name    string `json:"name"`
		Created string `json:"created"`
		Source  string `json:"source"`
		Expires string `json:"expires"`
		Reason  string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(output), &source); err != nil {
		return nil, errors.New("docker effects: invalid observed ban snapshot")
	}
	if len(source) > 1000 {
		return nil, errors.New("docker effects: observed ban snapshot exceeds limit")
	}
	result := make([]effect.FleetRuntimeAccessSnapshotBanV1, 0, len(source))
	for _, item := range source {
		result = append(result, effect.FleetRuntimeAccessSnapshotBanV1{
			UUID: strings.TrimSpace(item.UUID), Name: strings.TrimSpace(item.Name), Created: strings.TrimSpace(item.Created),
			Source: strings.TrimSpace(item.Source), Expires: strings.TrimSpace(item.Expires), Reason: strings.TrimSpace(item.Reason),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].UUID < result[j].UUID })
	return result, nil
}

func parseRuntimeObservationArtifacts(datapackOutput, modOutput string) ([]effect.FleetRuntimeObservedArtifactV1, error) {
	result := make([]effect.FleetRuntimeObservedArtifactV1, 0)
	seen := make(map[string]struct{})
	for _, source := range []struct{ output, extension, kind string }{{datapackOutput, ".zip", "datapack"}, {modOutput, ".jar", "mod"}} {
		for _, line := range strings.Split(source.output, "\n") {
			name := strings.TrimSpace(strings.TrimSuffix(line, "\r"))
			if name == "" || !strings.HasSuffix(strings.ToLower(name), source.extension) {
				continue
			}
			item := effect.FleetRuntimeObservedArtifactV1{Name: name, Kind: source.kind}
			if err := item.Validate(); err != nil {
				return nil, err
			}
			key := source.kind + "\x00" + name
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			result = append(result, item)
			if len(result) > fleetObservationMaxArtifacts {
				return nil, errors.New("docker effects: observed artifacts exceed limit")
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Kind == result[j].Kind {
			return result[i].Name < result[j].Name
		}
		return result[i].Kind < result[j].Kind
	})
	return result, nil
}
