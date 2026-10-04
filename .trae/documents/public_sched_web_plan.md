# sched 管理台公网暴露计划

## 现状诊断

| 项 | 状态 |
|---|---|
| sched serve | PID=1568485，监听 `127.0.0.1:8765` |
| 机器 IP | AWS VPC 内网 `172.31.169.116`，无公网 IP |
| 公网出口 | ifconfig.me 超时（VPC 无 NAT 网关或禁外连） |
| cloudflared/ngrok | 未装、大概率装不了（禁外连） |
| WireGuard server | 已装（systemd disabled），xray-easy/ip-tunnel skill 可用 |
| 当前 web 安全 | `host=127.0.0.1` + `auth_token=""` — 任何能连到该主机的进程都能打开管理台，等同 RCE |

## 计划

### 阶段 1：服务器侧安全加固（我能做的确定项）

1. 杀现有 sched serve（PID 1568485）
2. 改 `data/settings.json`：
   - `web.host: "0.0.0.0"`（监听所有网卡）
   - `web.auth_token` 设一个随机 32 位 hex（例如 `openssl rand -hex 16`）
   - `allow_public_no_auth: false` 保持（防意外）
3. 清 scheduler.lock，重启 serve --web
4. curl 验证：
   - 无 token → 401 unauthorized
   - 有 token header → 200 正常
   - `ss -tlnp` 确认 `0.0.0.0:8765` 监听

### 阶段 2：公网暴露路径（需要用户选）

选项 A — **AWS 安全组 + 弹性 IP（最直接）**
- 用户去 AWS console：给实例绑弹性 IP、安全组开 TCP 8765（源 IP 建议仅限你的公网出口 IP，别开 0.0.0.0/0）
- 完成后 URL = `http://<弹性IP>:8765/api/status` + header `Authorization: <token>`

选项 B — **SSH 反向隧道（需要跳板机）**
- 你提供一台有公网 IP 的跳板机（或 Cloudflare Tunnel + SSH 端口转发）
- 我生成命令：`ssh -R 0.0.0.0:8765:localhost:8765 -N -f <user>@<jumpbox>`

选项 C — **cloudflared 隧道（需要外网可达 + Cloudflare 账号）**
- 大概率禁外连装不了 → 作为备选，如果 ping cloudflare.com 通就装

### 阶段 3：可选——反向代理 + HTTPS（如果要长期用）

nginx / traefik / Caddy 反代 8765 → 配 Let's Encrypt 免费证书 → 用 Cloudflare Tunnel 暴露（自动 HTTPS + 免开放 AWS 端口）。阶段 1+2 跑通后再做。

## 文件改动

- `data/settings.json`：改 web.host / web.auth_token（重启后生效）
- 无代码改动

## 依赖

- 服务器能重启进程（当前 sched serve 是前台 nohup）
- 用户选定公网路径并提供所需凭据（弹性 IP / 跳板机 / Cloudflare 账号）

## 风险 & 处理

- **公网出口被禁导致 cloudflared 装不了** → 直接回退选 A/B
- **AWS 安全组开错源 IP 导致管理台被外网扫** → auth_token 认证兜底（选项 A 的安全关键）
- **机器无 root 权限改 systemd 或装 cloudflared** → 确认以当前用户运行即可
