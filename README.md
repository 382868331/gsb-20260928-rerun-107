# rdt — 可靠数据传输库（Go）

在不可靠（丢包、延迟、乱序、重复）的数据报通道上提供面向连接的、可靠的、按序的字节流，
并在一条连接内复用多条**双向独立流**，带流级流控与公平带宽分配。

## 包结构

- `rdt/`：协议核心
  - `frame.go`：帧类型（SYN/SYNACK/DATA/ACK/FIN/FINACK/RST）、`StreamID` 与 `Transport` 接口。
  - `config.go`：MSS、每流发送/接收缓存、握手超时、RTO、关闭宽限、最大流数。
  - `conn.go`：多路复用连接 `Conn`、epoch、读写 goroutine、出站优先级队列。
  - `conn_handshake.go`：带随机 epoch 的会话建立与握手重传。
  - `conn_loop.go`：单事件循环（收包、RTO、零窗口探测、**公平轮转调度**）。
  - `stream.go` / `stream_send.go`：单条流的序号空间、接收缓存、按序交付、
    `Read`/`Write`、FIN 半关闭、RST 取消、`CloseAndWait`。
  - `conn_close.go`：流墓碑（tombstone）与连接级取消收尾。
  - `session.go`：单流外观 `Session`（`Dial`/`Accept`/`Read`/`Write`/`Close`），
    内部就是引导流（id 1）上的一个 `Conn`。
- `sim/`：可重放的丢包/延迟/重复模拟链路，支持故障决策日志与脚本重放。

## 多流语义

- **双向独立流**：`DialConn`/`AcceptConn` 建立连接，`OpenStream` 打开流，
  `AcceptStream` 接收对端新流。奇数 id 属于客户端、偶数 id 属于服务端，两方可同时开流。
- **按流隔离的流控**：每条流有独立序号空间与独立接收窗口（`rcvNxt` + 有界 `rcvHold`）。
  一条流读得慢只会关掉自己的窗口；发送方 `Write` 在该流的 `SendBuffer` 上阻塞，
  其他流照常收发，慢流数据不会被无限缓存。
- **公平带宽**：事件循环以轮转（round-robin）方式服务所有可发送流，每轮每条就绪流
  至多发送一个 MSS 段；零窗口的流跳过，不让慢流堵死快流。
- **单流取消**：`Stream.Close()` 立即释放该流全部缓存并发送 RST，阻塞中的
  `Read`/`Write` 解开；同连接的其他流不受影响。
- **半关闭**：`CloseWrite()` 只结束发送方向，排队字节与 FIN 仍可靠送达，`Read` 继续
  接收对端尚未传完的内容直到 FIN/RST。FIN 严格排在所有数据之后，双向 FIN 用各自
  发送方向的序号独立确认（FinAck.Ack = 对端 FIN seq + 1）。
- **优雅全关闭**：`Stream.CloseAndWait(ctx)` 半关发送方向、等 FinAck 并收完反向数据；
  ctx 取消只 RST 这一条流。
- **连接重建隔离（epoch）**：SYN 携带随机 epoch，服务端采用客户端 epoch。旧会话的任何
  DATA/ACK/FIN 与新 epoch 不符一律丢弃，旧确认、旧数据不能成为新流的进度。
  完全结束的流记入有界墓碑，迟到重复帧不会让旧流复活。

## 模拟与精确重放

`sim.Config` 支持 `LossRate`、`DupRate` 与均匀延迟（乱序来自每帧独立延迟与重复副本）。
`sim.Options{Journal, Replay}` 可记录每帧的丢包/重复/延迟决策，并以同一脚本精确重放：

```go
journal := &sim.Journal{}
pair := sim.NewPairOptions(seed, cfgAB, cfgBA, sim.Options{Journal: journal})
// ... 运行场景 ...
again := sim.NewPairOptions(seed, cfgAB, cfgBA, sim.Options{Replay: sim.NewReplay(journal.Snapshot())})
```

## 使用示例

```go
// 多条独立流
cli, _ := rdt.DialConn(ctx, pair.A, cfg)
s1, _ := cli.OpenStream()
s2, _ := cli.OpenStream()
go s1.Write(bigSlowStream)   // 对端读得慢：只阻塞 s1
s2.Write(latencySensitive)   // s2 不受影响
```

## 测试

```sh
go test ./...          # 功能
go test ./... -race    # 竞态
```

- `TestOrderedBytesUnderLoss` / `TestNoDuplicateDelivery`：多种子丢包/乱序/重复下逐字节核对。
- `TestReplayable` / `TestFaultDecisionReplay`：相同种子与故障决策脚本精确复现。
- `TestMultiStreamIndependent`：4 条流并发，各自字节精确，且双向半关闭收发回复。
- `TestSlowStreamDoesNotBlockOthers`：慢流 1 字节级读取时，快流仍迅速完成。
- `TestSlowStreamBoundedBuffers`：从不读取的流在窗口打满后阻塞 `Write`，不无限缓存；RST 解开。
- `TestStreamCancelIsolation` / `TestStreamCloseAndWaitGraceful`：取消隔离与优雅全关闭。
- `TestHalfCloseAllowsReverseData`：单方向关闭后对向数据完整到达。
- `TestEpochIsolation` / `TestReconnectStaleFramesIgnored`：小缓存反复关闭重连，旧帧无效。
