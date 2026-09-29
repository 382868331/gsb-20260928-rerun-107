# rdtlib — 可靠数据传输库（Go）

在不可靠的本地模拟通道（随机丢包、重复、乱序、延迟，全程开启）之上，提供
会话建立、有限发送窗口、累计确认、超时重传，以及有序且不重复的字节交付。
会话与确认语义独立于未来的拥塞策略，后续可在发送窗口决策层叠加拥塞控制，
而不改变握手、确认、纪元隔离与关闭语义。

## 包结构

- `rdt/protocol.go`：定长报文头与 `SYN/ACK/FIN/RST` 标志；SYN、FIN 各占
  一个逻辑序号。
- `rdt/transport.go`：`FaultyTransport` 与 `FaultProfile`。故障序列由
  `(seed, seed)` 播种的 PCG 决定，相同 seed 可完全重放；通道缓冲有上界。
- `rdt/conn.go`：`Conn` 会话。
  - 发送端：有界窗口令牌 + 最早未确认段定时器，超时按 go-back-N 重传，
    RTO 按重传次数指数退避、在确认推进后复位。
  - 接收端：`rcvNxt` 吸收重复与重传；乱序段进入有界缓存，仅连续字节进入
    读缓冲，因此重复确认、乱序、重传都不会重复交付。
  - 纪元（epoch）：每次新拨号生成随机 64 位纪元，双向报文都携带；旧连接
    的在途数据/FIN/RST 在新连接中被直接丢弃。
  - 关闭：`CloseWrite` 半关闭，`Close` 优雅关闭，`Cancel` 发 RST 立即收尾；
    优雅关闭后有 TIME_WAIT 风格 linger，周期重发最后 ACK，保证最后一个
    FIN 的确认丢失也能恢复；空发送方向不产生线网上的 FIN。
- `rdt/mux.go`：按 `(对端地址, epoch)` 多路分离；拨号侧在首个 SYN 时注册
  会话，握手 SYN-ACK 与后续数据都进同一会话。
- `rdt/listener.go`：监听器。每个 epoch 至多一个会话；重复 SYN 只重发
  SYN-ACK；会话在首个 SYN 时创建，数据与最终握手 ACK 同批到达也不丢。

## 使用方式

```go
prof := rdt.NewFaultProfile(42)
prof.LossProbability = 0.35
tr := rdt.NewFaultyTransport(prof)
defer tr.Close()

mux := rdt.NewMux(tr, "client", "server")
lis := rdt.Listen(mux, "server", rdt.Config{MSS: 512, SendWindow: 8})

go func() {
    conn, _ := lis.Accept(ctx)
    b, _ := io.ReadAll(conn)
    conn.CloseWrite(ctx)
    _ = b
}()

c, _ := mux.Dial(ctx, "client", "server", rdt.Config{MSS: 512, SendWindow: 8})
c.Write(ctx, data)
c.Close(ctx)
```

## 验证

`go test ./...`（也可加 `-race`）覆盖：

- 多个 seed 下故障全程开启的 5000 字节按序、恰好一次交付与最终 SHA-256
  校验；测试不会在关闭故障后才得到正确结果。
- 相同 seed 的故障决策序列与结果可重放。
- 取消旧连接并重建后，旧 epoch 的报文不能污染新流。
- 取消后两侧会话 goroutine 都退出（`Done` 关闭）。
- 接收乱序缓存受 `MSS*SendWindow` 上界约束。
