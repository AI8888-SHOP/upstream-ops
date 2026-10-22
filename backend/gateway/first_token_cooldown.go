package gateway

import "github.com/bejix/upstream-ops/backend/storage"

// firstTokenCooldownExempt 判断该路由是否在首字超时冷却白名单里。
// 命中白名单的路由即使首字超时/首字预算失败也不会进入模型冷却，
// 避免兜底渠道因为一次慢响应被整体摘除。
func firstTokenCooldownExempt(route *storage.GatewayRoute) bool {
	return route != nil && route.FirstTokenCooldownExempt
}
