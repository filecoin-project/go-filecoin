package jsonrpc

import (
	jstd "github.com/filecoin-project/go-jsonrpc"
)

// 这里的处理方式: 1.保持函数签名与官方库一致, 2.通过类型别名和重导入保持使用方式一致
type Config = jstd.Config
type Option = jstd.Option

var (
	WithReconnectBackoff = jstd.WithNoReconnect
	WithPingInterval     = jstd.WithPingInterval
	WithTimeout          = jstd.WithTimeout
	WithNoReconnect      = jstd.WithNoReconnect
	WithParamEncoder     = jstd.WithParamEncoder
)
