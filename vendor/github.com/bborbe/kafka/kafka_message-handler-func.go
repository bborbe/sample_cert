// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"

	"github.com/IBM/sarama"
)

// MessageHandlerFunc allow use a function as MessageHandler.
type MessageHandlerFunc func(ctx context.Context, msg *sarama.ConsumerMessage) error

// ConsumeMessage forward to the function.
func (m MessageHandlerFunc) ConsumeMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	return m(ctx, msg)
}
