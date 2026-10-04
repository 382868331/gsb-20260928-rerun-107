# rdt — 可靠数据传输库（Go）

在不可靠（丢包、延迟、乱序、重复）的数据报通道上提供面向连接的、可靠的、按序的字节流，
并在一个会话内多路复用多条双向独立流。

## 包结构

- `rdt/`：协议核心
  - `frame.go`：帧类型（SYN/SYNACK/DATA/ACK/FIN/FINACK）与 `Transport` 接口。
  - `config.go`：MSS、发送/接收缓存、握手超时、RTO、关闭宽限等配置。
  - `session.go`：`Session`、握手 epoch、读写 goroutine 与出站队列。
  - `handshake.go`：带 epoch 的会话建立与握手重传。
  - `loop.go`：单一事件循环，处理收包与定时重传。
  - `send.go`：发送窗口、累积 ACK、RTO 重传、零窗口探测、接收缓存、按序交付、`Read`/`Write`。
- `stream.go`：独立流状态、流级 RST 取消与每流读写 API。
- `group.go`：业务组共享额度、加权 DRR 调度、运行中调整与按组/按流核对。
- `close.go`：默认流 FIN/FINACK 半关闭、连接级 Abort、被动端静默宽限。
- `sim/`：可重放的丢包/延迟/重复模拟链路 `sim.NewPair(seed, ab, ba)`，
  `Link.Decisions()` 记录每帧故障决策，便于精确重放。

## 保证的语义

- **按序、不重复交付**：接收端以 `rcvNxt` 为界，乱序段进入有上限的 `rcvHold`；
  重复/重叠段被去重裁剪，只有连续字节进入应用缓冲区。
- **累积确认 + 超时重传**：每个数据段独立计时，RTO 到期重传未确认段；
  重复 ACK 不会造成重复交付。
- **连接重建隔离（epoch）**：SYN 携带随机 epoch，服务端采用客户端 epoch；
  旧连接残留的任何帧 epoch 不符一律丢弃，旧包不能污染新会话的数据。
- **有界缓存与流控**：`SendBuffer` 限制发送端排队+在途字节，`RecvBuffer` 限制
  接收端按序+乱序缓存，接收窗口随 ACK 通告；窗口更新丢失由零窗口探测恢复。
- **独立多路复用流**：`OpenStream`/`AcceptStream` 得到独立 `Stream`，各流有独立
  序号、ACK、FIN、RST 和有界接收/发送缓存；发送器按流轮询，慢读或取消一条流
  不会堵死其他正常流，也不会让慢流数据无限缓存。
- **业务组共享额度**：连接内流按业务组（`Stream.SetGroup`/`Session.SetGroup`）
  共享总额度（`Config.GroupQuota`，默认 `SendBuffer`）。组间按权重做亏空轮询
  （DRR）调度：交互组（高权重）优先且份额更大，但每组每轮都保有其权重份额，
  优先组持续繁忙也不会饿死批量组。`SetGroupWeight`/`SetTotalQuota` 可在传输
  进行中调整；在途字节继续计入占用，调低额度只是暂停新发送直到在途排空，
  接收窗口上限不变，慢接收者的积压不会因此突破 `RecvBuffer`。
- **换组不转移占用**：`SetGroup` 只影响之后的写入；已排队/在途字节按发送时
  的组归属，确认与取消（RST/Abort）释放的占用也记回原组，反复换组不能
  套取额外额度。`Session.GroupStats`/`StreamStats`（`Stream.Stats`）提供按组
  和按流的已发送、待确认、已丢弃字节核对；配合 `sim.Link.Decisions()` 可
  重放同一故障过程，解释公平等待与最终 FIN 收尾。
- **半关闭与取消**：`Close`/`CloseWrite` 只结束发送方向，仍可读取对向尚未完成的数据；
  `Stream.Close` 仅 RST 当前流，`Session.Abort` 或取消等待才终止整个连接。
- **故障全程开启**：测试在整个传输过程中保持丢包/延迟，从不通过“关闭故障”
  得到正确结果；同一种子可精确重放同一场景。

## 使用示例

```go
pair := sim.NewPair(seed,
    sim.Config{LossRate: 0.3, MinDelay: 1*time.Millisecond, MaxDelay: 9*time.Millisecond},
    sim.Config{LossRate: 0.3, MinDelay: 1*time.Millisecond, MaxDelay: 9*time.Millisecond})

// 服务端
go func() {
    srv, _ := rdt.Accept(ctx, pair.B, cfg)
    defer srv.Close(context.Background())
    io.Copy(dst, srv) // 按序读到 EOF
}()

// 客户端
cli, _ := rdt.Dial(ctx, pair.A, cfg)
cli.Write(data)
cli.Close(context.Background()) // 冲刷 + FIN，等待 FinAck
```

## 测试

```sh
go test ./...            # 功能
go test ./... -race     # 竞态
```

- `TestOrderedBytesUnderLoss`：多种子 30% 丢包 + 延迟/乱序，逐字节核对 SHA256。
- `TestReplayable`：相同种子复现相同结果。
- `TestEpochIsolation`：同传输重建后，注入旧 epoch 帧不污染新数据。
- `TestCancellationWindsDown`：中途取消，两边都解开。
- `TestBoundedBuffersComplete`：小缓存 + 慢速应用 + 丢包下仍完整交付。
- `TestNoDuplicateDelivery`：45% 丢包下无重复/乱序交付。
- `TestIndependentStreamsFairAndBounded`：小缓存下慢流不阻塞正常流。
- `TestStreamResetDoesNotBlockOtherStream`：单流 RST 不影响其他流。
- `TestGroupSharedQuotaFairProgress`：交互/批量组共享小额度，批量组不被饿死。
- `TestGroupSwitchAccounting`：连续换组后按组/按流核对发送、待确认、丢弃。
- `TestGroupResetAccounting`：换组后取消，占用与丢弃仍记回原组。
- `TestRuntimeQuotaAndWeightAdjust`：传输中调整组权重与总额度仍完整交付。
- `TestGroupHalfCloseReplay`：换组 + 双向半关闭逐字节核对，同种子重放故障决策。
- `TestHalfCloseKeepsReceiving`：半关闭后继续接收对向数据。
- `TestDeterministicDuplicatesRecorded`：重复包下字节正确，故障决策可读取。
