package repository

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

const (
	settingReadCacheEnvTTL = "SETTING_READ_CACHE_TTL_SECONDS"
	settingReadCacheMaxTTL = 24 * time.Hour
)

type cachedSettingRepository struct {
	base service.SettingRepository
	ttl  time.Duration

	mu      sync.RWMutex
	values  map[string]cachedSettingValue
	all     map[string]string
	allExp  time.Time
	started bool
}

type cachedSettingValue struct {
	setting   *service.Setting
	expiresAt time.Time
}

func NewCachedSettingRepository(base service.SettingRepository) service.SettingRepository {
	if base == nil {
		return nil
	}
	ttl := settingReadCacheTTL()
	if ttl <= 0 {
		return base
	}
	return &cachedSettingRepository{
		base:   base,
		ttl:    ttl,
		values: make(map[string]cachedSettingValue),
	}
}

func settingReadCacheTTL() time.Duration {
	raw := strings.TrimSpace(os.Getenv(settingReadCacheEnvTTL))
	if raw == "" {
		return 0
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return 0
	}
	ttl := time.Duration(seconds) * time.Second
	if ttl > settingReadCacheMaxTTL {
		return settingReadCacheMaxTTL
	}
	return ttl
}

func (r *cachedSettingRepository) Get(ctx context.Context, key string) (*service.Setting, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return r.base.Get(ctx, key)
	}
	if setting, ok := r.getCached(key); ok {
		if setting == nil {
			return nil, service.ErrSettingNotFound
		}
		return cloneSetting(setting), nil
	}

	setting, err := r.base.Get(ctx, key)
	if err != nil {
		if errors.Is(err, service.ErrSettingNotFound) {
			r.setCached(key, nil)
		}
		return nil, err
	}
	r.setCached(key, setting)
	return cloneSetting(setting), nil
}

func (r *cachedSettingRepository) GetValue(ctx context.Context, key string) (string, error) {
	setting, err := r.Get(ctx, key)
	if err != nil {
		return "", err
	}
	return setting.Value, nil
}

func (r *cachedSettingRepository) Set(ctx context.Context, key, value string) error {
	if err := r.base.Set(ctx, key, value); err != nil {
		return err
	}
	r.mu.Lock()
	r.values[strings.TrimSpace(key)] = cachedSettingValue{
		setting:   &service.Setting{Key: strings.TrimSpace(key), Value: value, UpdatedAt: time.Now()},
		expiresAt: time.Now().Add(r.ttl),
	}
	r.all = nil
	r.allExp = time.Time{}
	r.mu.Unlock()
	return nil
}

func (r *cachedSettingRepository) GetMultiple(ctx context.Context, keys []string) (map[string]string, error) {
	if len(keys) == 0 {
		return map[string]string{}, nil
	}

	result := make(map[string]string)
	missing := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, rawKey := range keys {
		key := strings.TrimSpace(rawKey)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if setting, ok := r.getCached(key); ok {
			if setting != nil {
				result[key] = setting.Value
			}
			continue
		}
		missing = append(missing, key)
	}

	if len(missing) == 0 {
		return result, nil
	}

	loaded, err := r.base.GetMultiple(ctx, missing)
	if err != nil {
		return nil, err
	}
	found := make(map[string]struct{}, len(loaded))
	for key, value := range loaded {
		key = strings.TrimSpace(key)
		result[key] = value
		found[key] = struct{}{}
		r.setCached(key, &service.Setting{Key: key, Value: value, UpdatedAt: time.Now()})
	}
	for _, key := range missing {
		if _, ok := found[key]; !ok {
			r.setCached(key, nil)
		}
	}
	return result, nil
}

func (r *cachedSettingRepository) SetMultiple(ctx context.Context, settings map[string]string) error {
	if err := r.base.SetMultiple(ctx, settings); err != nil {
		return err
	}
	now := time.Now()
	r.mu.Lock()
	for key, value := range settings {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		r.values[key] = cachedSettingValue{
			setting:   &service.Setting{Key: key, Value: value, UpdatedAt: now},
			expiresAt: now.Add(r.ttl),
		}
	}
	r.all = nil
	r.allExp = time.Time{}
	r.mu.Unlock()
	return nil
}

func (r *cachedSettingRepository) GetAll(ctx context.Context) (map[string]string, error) {
	now := time.Now()
	r.mu.RLock()
	if r.all != nil && now.Before(r.allExp) {
		copied := cloneStringMap(r.all)
		r.mu.RUnlock()
		return copied, nil
	}
	r.mu.RUnlock()

	all, err := r.base.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	expiresAt := now.Add(r.ttl)
	r.mu.Lock()
	r.all = cloneStringMap(all)
	r.allExp = expiresAt
	for key, value := range all {
		r.values[key] = cachedSettingValue{
			setting:   &service.Setting{Key: key, Value: value, UpdatedAt: now},
			expiresAt: expiresAt,
		}
	}
	if !r.started {
		r.started = true
		log.Printf("[settings-cache] read cache enabled, ttl=%s", r.ttl)
	}
	r.mu.Unlock()
	return cloneStringMap(all), nil
}

func (r *cachedSettingRepository) Delete(ctx context.Context, key string) error {
	if err := r.base.Delete(ctx, key); err != nil {
		return err
	}
	key = strings.TrimSpace(key)
	r.mu.Lock()
	r.values[key] = cachedSettingValue{expiresAt: time.Now().Add(r.ttl)}
	r.all = nil
	r.allExp = time.Time{}
	r.mu.Unlock()
	return nil
}

func (r *cachedSettingRepository) getCached(key string) (*service.Setting, bool) {
	now := time.Now()
	r.mu.RLock()
	item, ok := r.values[key]
	if ok && now.Before(item.expiresAt) {
		setting := cloneSetting(item.setting)
		r.mu.RUnlock()
		return setting, true
	}
	r.mu.RUnlock()
	if ok {
		r.mu.Lock()
		if current, exists := r.values[key]; exists && !now.Before(current.expiresAt) {
			delete(r.values, key)
		}
		r.mu.Unlock()
	}
	return nil, false
}

func (r *cachedSettingRepository) setCached(key string, setting *service.Setting) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	r.mu.Lock()
	r.values[key] = cachedSettingValue{
		setting:   cloneSetting(setting),
		expiresAt: time.Now().Add(r.ttl),
	}
	if !r.started {
		r.started = true
		log.Printf("[settings-cache] read cache enabled, ttl=%s", r.ttl)
	}
	r.mu.Unlock()
}

func cloneSetting(in *service.Setting) *service.Setting {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
