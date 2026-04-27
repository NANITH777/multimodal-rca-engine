"""
Log Generator Module for the Multimodal RCA Engine.
Generates realistic synthetic log entries for all 11 infrastructure layers.
Each log session is temporally aligned with generated metrics.
"""

import random
import string
from datetime import datetime, timedelta


# ============================================
# Log Templates per Layer (realistic patterns)
# ============================================

LOG_TEMPLATES = {
    "cdn": {
        "INFO": [
            "CDN edge node {node} served request for {path} in {latency}ms (cache: {cache_status})",
            "Cache purge completed for zone {zone}, objects invalidated: {count}",
            "Traffic routed to origin {origin} for path {path}, ttl={ttl}s",
            "Health check passed for edge node {node}, latency={latency}ms",
            "SSL certificate renewed for domain {domain}, expires={expiry}",
            "CDN configuration reloaded successfully on node {node}",
            "Bandwidth allocation updated for zone {zone}: {bandwidth}Mbps",
        ],
        "WARN": [
            "Cache miss rate elevated on node {node}: {rate}% (threshold: 20%)",
            "Origin server {origin} response time degraded: {latency}ms",
            "Rate limiting triggered for client IP {ip}: {count} req/s exceeded threshold",
            "CDN edge node {node} approaching connection limit: {connections}/{max_connections}",
            "Stale content served for {path}, origin unreachable for {duration}s",
        ],
        "ERROR": [
            "Connection to origin {origin} failed: {error_code} {error_msg}",
            "CDN edge node {node} returned 5xx for {count} requests in last {window}s",
            "SSL handshake failed for domain {domain}: certificate mismatch",
            "DDoS mitigation activated: {count} suspicious requests from {ip_range}",
            "Cache corruption detected on node {node}, initiating rebuild for zone {zone}",
        ],
        "CRITICAL": [
            "CDN edge node {node} UNRESPONSIVE — failover to {backup_node} initiated",
            "DDoS attack detected: {rate} req/s from {source_count} unique IPs targeting {path}",
            "Origin server {origin} DOWN — all cached content serving stale data",
            "Complete cache invalidation triggered on node {node} — performance impact expected",
        ],
    },
    "firewall": {
        "INFO": [
            "Firewall rule {rule_id} matched: ALLOW {proto} {src_ip}:{src_port} -> {dst_ip}:{dst_port}",
            "Connection tracking table cleanup: removed {count} expired entries",
            "Firewall policy {policy} loaded successfully, {rule_count} rules active",
            "NAT translation established: {src_ip} -> {nat_ip} for session {session_id}",
            "Geo-blocking updated: {count} IP ranges added for region {region}",
        ],
        "WARN": [
            "Connection tracking table at {usage}% capacity ({count}/{max_count} entries)",
            "Port scan detected from {src_ip}: {port_count} ports probed in {window}s",
            "Firewall rule {rule_id} hit rate abnormally high: {hits}/min",
            "DNS query flood detected from {src_ip}: {qps} queries/sec",
            "Fragmented packet reassembly timeout for {src_ip}: possible evasion attempt",
        ],
        "ERROR": [
            "BLOCKED: {proto} connection from {src_ip}:{src_port} -> {dst_ip}:{dst_port} (rule: {rule_id})",
            "IPS alert: Signature {sig_id} triggered — {attack_type} from {src_ip}",
            "Firewall failover activated: primary interface {iface} down",
            "Rate limit exceeded for {src_ip}: {count} connections in {window}s, temporary ban applied",
            "Invalid packet dropped: {proto} from {src_ip}, flags={flags}",
        ],
        "CRITICAL": [
            "CRITICAL: Brute force attack detected from {src_ip}: {count} failed auth attempts in {window}s",
            "Firewall rules corruption detected — falling back to default deny policy",
            "Connection table OVERFLOW: {count} entries, new connections being DROPPED",
            "Suspected data exfiltration: {bytes}MB transferred to {dst_ip} in {duration}min",
        ],
    },
    "proxy": {
        "INFO": [
            "Proxy {proxy_id} forwarded request: {method} {url} -> {backend}:{port} ({latency}ms)",
            "Backend {backend} health check passed: latency={latency}ms, status={status}",
            "Connection pool for {backend}: active={active}, idle={idle}, max={max_pool}",
            "Request re-routed from {backend_a} to {backend_b}: load balancing adjustment",
            "Proxy cache hit for {url}: served {size}KB in {latency}ms",
        ],
        "WARN": [
            "Backend {backend} response time elevated: {latency}ms (SLA: {sla}ms)",
            "Proxy connection pool exhaustion approaching: {usage}% utilized for {backend}",
            "Request retry #{attempt} for {method} {url}: backend {backend} returned {status_code}",
            "Slow response from backend {backend}: {latency}ms for {method} {url}",
            "Client connection timeout: {client_ip} waited {timeout}s for response",
        ],
        "ERROR": [
            "Backend {backend} returned {status_code}: {error_msg} for {method} {url}",
            "Proxy {proxy_id} upstream connection failed: {backend}:{port} — {error}",
            "SSL/TLS error with backend {backend}: {ssl_error}",
            "Request dropped: max retries ({max_retries}) exceeded for {method} {url}",
            "Proxy overloaded: {active_connections} active connections, rejecting new requests",
        ],
        "CRITICAL": [
            "Backend {backend} UNREACHABLE — removed from load balancer pool",
            "Proxy {proxy_id} OUT OF MEMORY: {memory_used}MB/{memory_total}MB",
            "All backends DOWN for service {service}: returning 503 to all clients",
            "Proxy crash detected on {proxy_id} — automatic restart initiated",
        ],
    },
    "kubernetes_ingress": {
        "INFO": [
            "Ingress {ingress} routing: {host}{path} -> {service}:{port} ({latency}ms)",
            "SSL certificate loaded for {host}: issuer={issuer}, expires={expiry}",
            "Ingress controller synced with {endpoint_count} endpoints for service {service}",
            "Rate limiting configured for {host}: {limit} req/s, burst={burst}",
            "Health check probe succeeded for {service}: status={status_code}, latency={latency}ms",
        ],
        "WARN": [
            "Ingress {ingress} backend {service} has {unhealthy_count} unhealthy endpoints",
            "TLS certificate for {host} expiring in {days} days",
            "Request rate approaching limit for {host}: {current_rate}/{limit} req/s",
            "Ingress latency spike: {host}{path} -> {service} took {latency}ms (p99: {p99}ms)",
            "Load balancer session affinity violation for {client_ip}: routed to different backend",
        ],
        "ERROR": [
            "Ingress {ingress}: upstream {service} returned {status_code} for {method} {host}{path}",
            "No healthy upstream for ingress {ingress}, service {service}: 0/{total} endpoints ready",
            "Ingress TLS handshake failed for {host}: {tls_error}",
            "Ingress rate limit exceeded for {client_ip}: {method} {host}{path} — 429 returned",
            "Ingress configuration error: duplicate path {path} for hosts {host_a}, {host_b}",
        ],
        "CRITICAL": [
            "Ingress controller CRASH: restarting — all routing temporarily disrupted",
            "Ingress {ingress} SERVICE UNAVAILABLE: all endpoints for {service} are DOWN",
            "Certificate expired for {host} — HTTPS connections failing",
            "Ingress controller OOM killed: memory={memory_used}Mi, limit={memory_limit}Mi",
        ],
    },
    "kubernetes_deployments": {
        "INFO": [
            "Pod {pod} (deployment/{deployment}) started successfully in {duration}s",
            "Deployment {deployment} scaled: {old_replicas} -> {new_replicas} replicas",
            "Container {container} in pod {pod} passed readiness probe, endpoint added",
            "Rolling update for {deployment}: {updated}/{total} pods updated",
            "HPA for {deployment}: current replicas={replicas}, target CPU={target_cpu}%",
        ],
        "WARN": [
            "Pod {pod} (deployment/{deployment}) restarted: count={restart_count}, reason={reason}",
            "Container {container} in pod {pod} OOMKilled: used={memory_used}Mi, limit={memory_limit}Mi",
            "Deployment {deployment} pod {pod} CPU throttled: usage={cpu_pct}%, limit={cpu_limit}m",
            "Pod {pod} scheduling delayed: insufficient {resource} on nodes",
            "Deployment {deployment} has {unavailable} unavailable replicas",
        ],
        "ERROR": [
            "Pod {pod} CrashLoopBackOff: {restart_count} restarts, backoff={backoff}s",
            "Container {container} in pod {pod} failed liveness probe: HTTP {status_code}",
            "Deployment {deployment} rollout FAILED: deadline exceeded after {duration}s",
            "ImagePullBackOff for pod {pod}: image {image} not found in registry",
            "Pod {pod} evicted: node {node} under {resource} pressure",
        ],
        "CRITICAL": [
            "Deployment {deployment}: 0/{total} replicas available — SERVICE DOWN",
            "Node {node} NotReady — {pod_count} pods from {deployment} being rescheduled",
            "Persistent volume claim {pvc} for {deployment} LOST — data unavailable",
            "Deployment {deployment} cascading failure: all pods in CrashLoopBackOff",
        ],
    },
    "application": {
        "INFO": [
            "Request processed: {method} {endpoint} -> {status_code} ({latency}ms, user={user_id})",
            "Database connection pool: active={active}/{max_pool}, idle={idle}, wait_queue={queue}",
            "Background job {job_id} completed: processed {count} records in {duration}s",
            "API rate limit check: user={user_id}, remaining={remaining}/{limit} requests",
            "Cache warmup completed for {cache_name}: {count} entries loaded in {duration}s",
        ],
        "WARN": [
            "Slow request: {method} {endpoint} took {latency}ms (threshold: {threshold}ms)",
            "Database connection pool near exhaustion: {active}/{max_pool} active, {queue} waiting",
            "API response time degraded: p95={p95}ms, p99={p99}ms (SLA: {sla}ms)",
            "Memory usage high: {memory_pct}% of allocated heap ({used}MB/{total}MB)",
            "Circuit breaker HALF-OPEN for service {service}: testing with {test_count} requests",
        ],
        "ERROR": [
            "Request failed: {method} {endpoint} -> {status_code}: {error_msg} (trace={trace_id})",
            "Database query timeout after {timeout}ms: {query_type} on table {table}",
            "Service {service} call failed: {error_type} — {error_msg} (retry {attempt}/{max})",
            "Unhandled exception in {handler}: {exception_type}: {exception_msg}",
            "Authentication failed for user {user_id}: {auth_error} from IP {client_ip}",
        ],
        "CRITICAL": [
            "SERVICE DEGRADED: error rate {error_rate}% exceeds threshold {threshold}%",
            "Database connection LOST: {db_host}:{db_port} — all queries failing",
            "Circuit breaker OPEN for service {service}: {failure_count} consecutive failures",
            "Out of memory: heap usage {heap_pct}% — potential memory leak in {component}",
        ],
    },
    "database": {
        "INFO": [
            "Query executed: {query_type} on {table} ({rows} rows, {duration}ms, conn={conn_id})",
            "Connection established: user={user}@{host}, database={database}, conn_id={conn_id}",
            "Checkpoint completed: {pages} pages written, {duration}ms elapsed",
            "Autovacuum: processed table {schema}.{table}, removed {dead_tuples} dead tuples",
            "Replication lag: {lag}ms, slave {slave_host} in sync with master",
        ],
        "WARN": [
            "Slow query ({duration}ms): {query_preview} [table: {table}, rows_examined: {rows}]",
            "Connection pool approaching limit: {active}/{max_connections} connections in use",
            "Lock wait timeout: transaction {txn_id} waiting on {table} for {wait_time}ms",
            "Replication lag increasing: {lag}ms (threshold: {threshold}ms)",
            "Table {schema}.{table} approaching max size: {size}GB/{limit}GB",
        ],
        "ERROR": [
            "Query failed: {error_code} — {error_msg} [query: {query_preview}]",
            "Deadlock detected between transactions {txn_a} and {txn_b} on table {table}",
            "Connection refused: max_connections ({max_connections}) reached",
            "Replication error: slave {slave_host} stopped — {repl_error}",
            "Corrupt index detected on {schema}.{table}.{index}: REINDEX required",
        ],
        "CRITICAL": [
            "Database server OUT OF DISK SPACE: {used}GB/{total}GB — writes blocked",
            "Master-slave replication BROKEN: {repl_error_count} errors, manual intervention needed",
            "Connection storm: {conn_count} connections in {window}s — possible DDoS",
            "Data corruption detected in table {schema}.{table}: {affected_rows} rows affected",
        ],
    },
    "storage": {
        "INFO": [
            "Disk {disk}: read {read_iops} IOPS, write {write_iops} IOPS, utilization {util}%",
            "Volume {volume} mounted at {mount_point}: {used}GB/{total}GB ({usage_pct}%)",
            "RAID array {array} scrub completed: no errors, duration={duration}min",
            "Storage pool {pool} thin provisioning: allocated={allocated}TB, used={used}TB",
            "Snapshot {snapshot_id} created for volume {volume}: size={size}GB",
        ],
        "WARN": [
            "Disk {disk} latency elevated: read={read_lat}ms, write={write_lat}ms (threshold: {threshold}ms)",
            "Volume {volume} usage at {usage_pct}%: {free}GB remaining",
            "SMART warning on disk {disk}: reallocated sectors count={count}",
            "I/O queue depth high on {disk}: {queue_depth} (normal: <{threshold})",
            "Storage throughput degraded: {current}MB/s vs expected {expected}MB/s",
        ],
        "ERROR": [
            "Disk {disk} I/O error: {error_type} at sector {sector} — retrying",
            "Volume {volume} FULL: 0 bytes remaining, writes blocked",
            "RAID array {array} degraded: disk {disk} failed, rebuilding on spare",
            "Storage controller {ctrl} timeout: {error_count} errors in {window}s",
            "Filesystem corruption detected on {mount_point}: fsck recommended",
        ],
        "CRITICAL": [
            "Disk {disk} FAILURE IMMINENT: SMART predicts failure within {days} days",
            "RAID array {array} CRITICAL: {failed_count} disks failed, NO SPARE available",
            "Storage pool {pool} at 100%: all writes blocked, data loss risk",
            "Multiple disk failures on {host}: {failed_count}/{total_count} disks offline",
        ],
    },
    "network": {
        "INFO": [
            "Interface {iface} link up: speed={speed}Gbps, duplex={duplex}",
            "ARP entry updated: {ip} -> {mac} on interface {iface}",
            "BGP session established with peer {peer_ip} (AS{as_number}): {route_count} routes",
            "VLAN {vlan_id} traffic: in={in_mbps}Mbps, out={out_mbps}Mbps, errors={errors}",
            "DNS resolution: {domain} -> {ip} ({latency}ms, server={dns_server})",
        ],
        "WARN": [
            "Interface {iface} packet loss: {loss_pct}% over last {window}s ({lost}/{total} packets)",
            "Network latency spike: {src} -> {dst} = {latency}ms (baseline: {baseline}ms)",
            "Interface {iface} CRC errors: {count} in last {window}s",
            "ARP storm detected on VLAN {vlan_id}: {arp_rate} ARP/s (normal: <{threshold})",
            "MTU mismatch detected: {iface_a}={mtu_a} vs {iface_b}={mtu_b}",
        ],
        "ERROR": [
            "Interface {iface} link DOWN: carrier lost, last up for {uptime}",
            "TCP retransmission rate high: {retrans_pct}% for {src} -> {dst}:{port}",
            "BGP session lost with peer {peer_ip} (AS{as_number}): {error_msg}",
            "Network unreachable: {dst_network} via {gateway} — {error}",
            "DNS resolution failed: NXDOMAIN for {domain} after {retries} retries",
        ],
        "CRITICAL": [
            "Network partition detected: {isolated_count} nodes unreachable from {src}",
            "Switch {switch} port {port} FLAPPING: {flap_count} state changes in {window}s",
            "Core router {router} UNRESPONSIVE — triggering failover to {backup}",
            "Complete network outage on VLAN {vlan_id}: 0 packets in/out for {duration}s",
        ],
    },
    "linux_vm": {
        "INFO": [
            "VM {hostname} CPU: user={user}%, sys={sys}%, idle={idle}%, load={load_avg}",
            "Memory: total={total}MB, used={used}MB ({usage_pct}%), free={free}MB, cached={cached}MB",
            "Process {process} (PID {pid}) started: user={user}, nice={nice}",
            "Cron job {job} completed successfully: exit_code=0, duration={duration}s",
            "Disk I/O: device={device}, read={read_mbps}MB/s, write={write_mbps}MB/s",
        ],
        "WARN": [
            "High CPU usage: {cpu_pct}% (user={user}%, sys={sys}%), load average: {load_1m}/{load_5m}/{load_15m}",
            "Memory pressure: {usage_pct}% used, swap usage: {swap_used}MB/{swap_total}MB",
            "Zombie processes detected: {zombie_count} zombies (PIDs: {pids})",
            "Disk I/O wait high: iowait={iowait}%, device={device}",
            "File descriptor limit approaching: {used}/{max} ({usage_pct}%)",
        ],
        "ERROR": [
            "OOM killer invoked: killed process {process} (PID {pid}), freed {freed}MB",
            "CPU thermal throttling: temperature={temp}°C, frequency reduced to {freq}MHz",
            "Filesystem /dev/{device} mounted on {mount_point}: read-only due to errors",
            "Process {process} (PID {pid}) segfault at {address} in {library}",
            "System call timeout: {syscall} hung for {duration}s in process {process}",
        ],
        "CRITICAL": [
            "KERNEL PANIC: {panic_msg} — system halted",
            "Memory exhaustion: OOM score adjustment failed, system unresponsive",
            "CPU usage at {cpu_pct}% for {duration}min — system overloaded",
            "Swap space exhausted: {swap_pct}% used — severe performance degradation",
        ],
    },
    "linux_host": {
        "INFO": [
            "Host {hostname} sensors: CPU temp={cpu_temp}°C, fan={fan_rpm}RPM, ambient={ambient_temp}°C",
            "IPMI: power consumption={power}W, inlet temp={inlet_temp}°C",
            "Disk health check: /dev/{disk} SMART status=PASSED, temp={disk_temp}°C",
            "Hardware inventory: {cpu_count}x {cpu_model}, {memory_gb}GB RAM, {disk_count} disks",
            "Firmware version: BIOS={bios_ver}, BMC={bmc_ver}, CPLD={cpld_ver}",
        ],
        "WARN": [
            "CPU temperature elevated: {cpu_temp}°C (warning threshold: {threshold}°C)",
            "Fan {fan_id} speed below normal: {fan_rpm}RPM (expected: >{min_rpm}RPM)",
            "SMART warning on /dev/{disk}: pending sectors={pending}, reallocated={reallocated}",
            "Power supply {psu_id} degraded: output={output}W/{rated}W, efficiency={eff}%",
            "Ambient temperature high: {ambient_temp}°C (threshold: {threshold}°C)",
        ],
        "ERROR": [
            "Disk /dev/{disk} SMART FAILING: error_count={errors}, predicted failure",
            "Fan {fan_id} FAILED: 0 RPM — compensating with remaining fans",
            "ECC memory error on DIMM {dimm}: correctable errors={count}",
            "Power supply {psu_id} failure — running on redundant PSU",
            "Hardware sensor alert: {sensor} reading {value} exceeds limit {limit}",
        ],
        "CRITICAL": [
            "CPU temperature CRITICAL: {cpu_temp}°C — automatic shutdown in {countdown}s",
            "Multiple disk failures: /dev/{disk_a}, /dev/{disk_b} — RAID degraded",
            "Uncorrectable ECC error on DIMM {dimm}: system stability compromised",
            "All fans failed on host {hostname}: THERMAL EMERGENCY",
        ],
    },
}

# ============================================
# Variable Generators for Template Filling
# ============================================

def _rand_ip():
    return f"{random.randint(10,192)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"

def _rand_hex(n=8):
    return ''.join(random.choices('0123456789abcdef', k=n))

def _rand_id(prefix="", length=6):
    return f"{prefix}{random.randint(10**(length-1), 10**length-1)}"

def _rand_pod():
    names = ["api-server", "web-frontend", "auth-service", "payment-svc", "order-processor",
             "notification-svc", "user-service", "gateway", "cache-layer", "worker"]
    return f"{random.choice(names)}-{_rand_hex(4)}-{_rand_hex(5)}"

def _rand_deployment():
    return random.choice(["api-server", "web-frontend", "auth-service", "payment-service",
                          "order-service", "notification-service", "user-service", "gateway-proxy",
                          "cache-service", "worker-pool", "metrics-collector", "log-aggregator"])

def _rand_backend():
    return f"{random.choice(['app','api','svc','backend'])}-{random.randint(1,20)}.internal"

def _rand_host():
    return f"node-{random.choice(string.ascii_lowercase)}{random.randint(1,50)}.dc{random.randint(1,3)}"

TEMPLATE_VARIABLES = {
    "node": lambda: f"edge-{random.choice(['us-east','eu-west','ap-south','us-west'])}-{random.randint(1,20)}",
    "path": lambda: random.choice(["/api/v1/users", "/static/js/app.js", "/api/v2/orders", "/images/logo.png",
                                    "/api/v1/products", "/health", "/api/v1/search", "/assets/styles.css"]),
    "latency": lambda: str(random.randint(1, 500)),
    "cache_status": lambda: random.choice(["HIT", "MISS", "BYPASS", "STALE"]),
    "zone": lambda: f"zone-{random.choice(['a','b','c','d'])}{random.randint(1,5)}",
    "count": lambda: str(random.randint(1, 5000)),
    "origin": lambda: f"origin-{random.randint(1,10)}.{random.choice(['us','eu','ap'])}.cloud",
    "ttl": lambda: str(random.choice([60, 300, 900, 3600, 86400])),
    "domain": lambda: f"{random.choice(['api','www','cdn','app'])}.{random.choice(['example','myapp','service'])}.com",
    "expiry": lambda: f"2026-{random.randint(6,12):02d}-{random.randint(1,28):02d}",
    "rate": lambda: f"{random.uniform(15,85):.1f}",
    "ip": _rand_ip,
    "ip_range": lambda: f"{random.randint(10,200)}.{random.randint(0,255)}.0.0/16",
    "connections": lambda: str(random.randint(100, 5000)),
    "max_connections": lambda: str(random.choice([5000, 10000, 20000])),
    "error_code": lambda: str(random.choice([500, 502, 503, 504, 408, 429])),
    "error_msg": lambda: random.choice(["Connection refused", "Timeout exceeded", "Internal server error",
                                        "Bad gateway", "Service unavailable", "Gateway timeout"]),
    "backup_node": lambda: f"edge-backup-{random.randint(1,5)}",
    "source_count": lambda: str(random.randint(50, 10000)),
    "window": lambda: str(random.choice([10, 30, 60, 300])),
    "bandwidth": lambda: str(random.randint(100, 2000)),
    "duration": lambda: str(random.randint(1, 3600)),
    # Firewall
    "rule_id": lambda: f"RULE-{random.randint(1000,9999)}",
    "proto": lambda: random.choice(["TCP", "UDP", "ICMP"]),
    "src_ip": _rand_ip,
    "dst_ip": _rand_ip,
    "src_port": lambda: str(random.randint(1024, 65535)),
    "dst_port": lambda: str(random.choice([22, 80, 443, 3306, 5432, 6379, 8080, 8443, 9090])),
    "policy": lambda: f"policy-{random.choice(['default','dmz','internal','external'])}",
    "rule_count": lambda: str(random.randint(50, 500)),
    "session_id": lambda: _rand_hex(12),
    "nat_ip": _rand_ip,
    "region": lambda: random.choice(["CN", "RU", "BR", "IN", "US", "EU"]),
    "port_count": lambda: str(random.randint(10, 1000)),
    "hits": lambda: str(random.randint(100, 50000)),
    "qps": lambda: str(random.randint(50, 5000)),
    "sig_id": lambda: f"SIG-{random.randint(10000,99999)}",
    "attack_type": lambda: random.choice(["SQL injection", "XSS", "Path traversal", "Buffer overflow",
                                          "Command injection", "LDAP injection"]),
    "iface": lambda: f"eth{random.randint(0,3)}",
    "flags": lambda: random.choice(["SYN", "FIN,ACK", "RST", "SYN,FIN", "URG,PSH"]),
    "bytes": lambda: str(random.randint(100, 50000)),
    # Proxy
    "proxy_id": lambda: f"proxy-{random.randint(1,10)}",
    "method": lambda: random.choice(["GET", "POST", "PUT", "DELETE", "PATCH"]),
    "url": lambda: f"/{random.choice(['api','service','app'])}/v{random.randint(1,3)}/{random.choice(['users','orders','products','health'])}",
    "backend": _rand_backend,
    "port": lambda: str(random.choice([80, 443, 3000, 8080, 8443, 9090])),
    "status": lambda: random.choice(["healthy", "degraded", "unknown"]),
    "active": lambda: str(random.randint(5, 200)),
    "idle": lambda: str(random.randint(0, 50)),
    "max_pool": lambda: str(random.choice([100, 200, 500])),
    "backend_a": _rand_backend,
    "backend_b": _rand_backend,
    "size": lambda: str(random.randint(1, 5000)),
    "sla": lambda: str(random.choice([100, 200, 500, 1000])),
    "usage": lambda: f"{random.randint(60,99)}",
    "attempt": lambda: str(random.randint(1, 5)),
    "max_retries": lambda: str(random.choice([3, 5])),
    "status_code": lambda: str(random.choice([200, 201, 400, 401, 403, 404, 500, 502, 503, 504])),
    "client_ip": _rand_ip,
    "timeout": lambda: str(random.choice([30, 60, 120, 300])),
    "active_connections": lambda: str(random.randint(500, 5000)),
    "ssl_error": lambda: random.choice(["certificate expired", "unknown CA", "handshake failure"]),
    "service": lambda: random.choice(["auth-svc", "payment-api", "order-mgr", "user-svc", "inventory-svc"]),
    "memory_used": lambda: str(random.randint(500, 8000)),
    "memory_total": lambda: str(random.choice([4096, 8192, 16384])),
    # K8s
    "ingress": lambda: f"ingress-{random.choice(['main','api','admin','public'])}",
    "host": lambda: f"{random.choice(['api','www','admin','app'])}.{random.choice(['example','myservice'])}.com",
    "endpoint_count": lambda: str(random.randint(2, 50)),
    "limit": lambda: str(random.choice([100, 500, 1000, 5000])),
    "burst": lambda: str(random.choice([10, 20, 50])),
    "unhealthy_count": lambda: str(random.randint(1, 5)),
    "days": lambda: str(random.randint(1, 30)),
    "current_rate": lambda: str(random.randint(400, 5000)),
    "p99": lambda: str(random.randint(200, 2000)),
    "total": lambda: str(random.randint(3, 20)),
    "tls_error": lambda: random.choice(["certificate verify failed", "unsupported protocol", "no cipher suites"]),
    "host_a": lambda: "api.example.com",
    "host_b": lambda: "legacy-api.example.com",
    "memory_limit": lambda: str(random.choice([256, 512, 1024, 2048])),
    # K8s Deployments
    "pod": _rand_pod,
    "deployment": _rand_deployment,
    "container": lambda: random.choice(["app", "sidecar", "init", "monitor"]),
    "old_replicas": lambda: str(random.randint(1, 10)),
    "new_replicas": lambda: str(random.randint(2, 20)),
    "updated": lambda: str(random.randint(1, 10)),
    "replicas": lambda: str(random.randint(1, 20)),
    "target_cpu": lambda: str(random.choice([50, 60, 70, 80])),
    "restart_count": lambda: str(random.randint(1, 50)),
    "reason": lambda: random.choice(["OOMKilled", "Error", "CrashLoopBackOff", "Completed"]),
    "memory_limit_mi": lambda: str(random.choice([256, 512, 1024])),
    "cpu_pct": lambda: f"{random.randint(50,99)}",
    "cpu_limit": lambda: str(random.choice([500, 1000, 2000, 4000])),
    "resource": lambda: random.choice(["cpu", "memory", "disk", "ephemeral-storage"]),
    "unavailable": lambda: str(random.randint(1, 5)),
    "backoff": lambda: str(random.choice([10, 20, 40, 80, 160, 300])),
    "image": lambda: f"registry.io/{random.choice(['app','api','worker'])}:{random.choice(['v1.2','v2.0','latest'])}",
    "pod_count": lambda: str(random.randint(3, 20)),
    "pvc": lambda: f"data-{random.choice(['postgres','redis','mongodb'])}-pvc-0",
    # Application
    "endpoint": lambda: f"/api/v{random.randint(1,3)}/{random.choice(['users','orders','products','search','auth'])}",
    "user_id": lambda: f"usr_{_rand_hex(8)}",
    "max_pool_app": lambda: str(random.choice([20, 50, 100])),
    "queue": lambda: str(random.randint(0, 50)),
    "job_id": lambda: f"job-{_rand_hex(6)}",
    "remaining": lambda: str(random.randint(0, 1000)),
    "cache_name": lambda: random.choice(["user-cache", "session-cache", "product-cache", "config-cache"]),
    "threshold": lambda: str(random.choice([100, 200, 500, 1000, 5000])),
    "p95": lambda: str(random.randint(100, 1500)),
    "memory_pct": lambda: f"{random.randint(60,98)}",
    "used": lambda: str(random.randint(100, 10000)),
    "free": lambda: str(random.randint(10, 5000)),
    "trace_id": lambda: _rand_hex(16),
    "error_type": lambda: random.choice(["ConnectionError", "TimeoutError", "HTTPError", "ValueError"]),
    "handler": lambda: random.choice(["UserController", "OrderService", "PaymentHandler", "AuthMiddleware"]),
    "exception_type": lambda: random.choice(["NullPointerException", "IndexOutOfRange", "KeyError", "TypeError"]),
    "exception_msg": lambda: random.choice(["null reference at line 42", "index 5 out of range", "key 'user' not found"]),
    "auth_error": lambda: random.choice(["invalid credentials", "token expired", "account locked"]),
    "error_rate": lambda: f"{random.uniform(5,50):.1f}",
    "failure_count": lambda: str(random.randint(5, 50)),
    "db_host": lambda: f"db-{random.choice(['primary','replica'])}-{random.randint(1,5)}.internal",
    "db_port": lambda: str(random.choice([3306, 5432, 27017])),
    "heap_pct": lambda: f"{random.randint(90,99)}",
    "component": lambda: random.choice(["UserService", "CacheManager", "SessionPool", "QueryEngine"]),
    # Database
    "query_type": lambda: random.choice(["SELECT", "INSERT", "UPDATE", "DELETE", "JOIN"]),
    "table": lambda: random.choice(["users", "orders", "products", "sessions", "transactions", "logs"]),
    "rows": lambda: str(random.randint(1, 100000)),
    "conn_id": lambda: str(random.randint(1, 10000)),
    "user": lambda: random.choice(["app_user", "readonly", "admin", "replication", "root"]),
    "database": lambda: random.choice(["production", "analytics", "staging", "auth_db"]),
    "pages": lambda: str(random.randint(100, 50000)),
    "schema": lambda: random.choice(["public", "app", "analytics"]),
    "dead_tuples": lambda: str(random.randint(100, 100000)),
    "lag": lambda: str(random.randint(0, 5000)),
    "slave_host": lambda: f"db-replica-{random.randint(1,5)}.internal",
    "query_preview": lambda: random.choice(["SELECT * FROM users WHERE...", "UPDATE orders SET status=...",
                                            "INSERT INTO transactions...", "DELETE FROM sessions WHERE..."]),
    "txn_id": lambda: f"txn-{_rand_hex(8)}",
    "txn_a": lambda: f"txn-{_rand_hex(8)}",
    "txn_b": lambda: f"txn-{_rand_hex(8)}",
    "wait_time": lambda: str(random.randint(1000, 30000)),
    "repl_error": lambda: random.choice(["duplicate key", "relay log corrupt", "connection lost"]),
    "index": lambda: f"idx_{random.choice(['users','orders','products'])}_{random.choice(['id','name','date'])}",
    "conn_count": lambda: str(random.randint(100, 5000)),
    "max_connections": lambda: str(random.choice([100, 200, 500, 1000])),
    "repl_error_count": lambda: str(random.randint(1, 50)),
    "affected_rows": lambda: str(random.randint(1, 10000)),
    # Storage
    "disk": lambda: f"sd{random.choice('abcdefgh')}",
    "read_iops": lambda: str(random.randint(100, 10000)),
    "write_iops": lambda: str(random.randint(50, 8000)),
    "util": lambda: f"{random.randint(10,95)}",
    "volume": lambda: f"vol-{_rand_hex(8)}",
    "mount_point": lambda: random.choice(["/data", "/var/lib/postgres", "/mnt/storage", "/opt/app"]),
    "total": lambda: str(random.randint(100, 10000)),
    "usage_pct": lambda: f"{random.randint(30,95)}",
    "array": lambda: f"md{random.randint(0,3)}",
    "pool": lambda: f"pool-{random.choice(['ssd','hdd','nvme'])}-{random.randint(1,3)}",
    "allocated": lambda: f"{random.uniform(1,50):.1f}",
    "snapshot_id": lambda: f"snap-{_rand_hex(8)}",
    "read_lat": lambda: str(random.randint(1, 100)),
    "write_lat": lambda: str(random.randint(1, 200)),
    "pending": lambda: str(random.randint(0, 50)),
    "reallocated": lambda: str(random.randint(0, 100)),
    "queue_depth": lambda: str(random.randint(16, 256)),
    "current": lambda: str(random.randint(50, 500)),
    "expected": lambda: str(random.randint(200, 1000)),
    "error_count": lambda: str(random.randint(1, 100)),
    "sector": lambda: str(random.randint(1000, 999999)),
    "ctrl": lambda: f"ctrl-{random.randint(0,3)}",
    "failed_count": lambda: str(random.randint(1, 4)),
    "total_count": lambda: str(random.randint(4, 24)),
    # Network
    "speed": lambda: random.choice(["1", "10", "25", "40", "100"]),
    "duplex": lambda: random.choice(["full", "half"]),
    "mac": lambda: ':'.join([_rand_hex(2) for _ in range(6)]),
    "peer_ip": _rand_ip,
    "as_number": lambda: str(random.randint(64512, 65534)),
    "route_count": lambda: str(random.randint(10, 5000)),
    "vlan_id": lambda: str(random.randint(1, 4094)),
    "in_mbps": lambda: str(random.randint(10, 1000)),
    "out_mbps": lambda: str(random.randint(10, 1000)),
    "errors": lambda: str(random.randint(0, 100)),
    "dns_server": lambda: random.choice(["8.8.8.8", "1.1.1.1", "208.67.222.222"]),
    "loss_pct": lambda: f"{random.uniform(0.1, 15):.2f}",
    "lost": lambda: str(random.randint(1, 1000)),
    "src": _rand_ip,
    "dst": _rand_ip,
    "baseline": lambda: str(random.randint(1, 20)),
    "arp_rate": lambda: str(random.randint(100, 10000)),
    "mtu_a": lambda: str(random.choice([1500, 9000])),
    "mtu_b": lambda: str(random.choice([1500, 9000])),
    "iface_a": lambda: "eth0",
    "iface_b": lambda: "eth1",
    "uptime": lambda: f"{random.randint(1,365)}d {random.randint(0,23)}h",
    "retrans_pct": lambda: f"{random.uniform(1,25):.1f}",
    "dst_network": lambda: f"{random.randint(10,172)}.{random.randint(0,255)}.0.0/16",
    "gateway": _rand_ip,
    "error": lambda: random.choice(["no route to host", "connection timed out", "network unreachable"]),
    "retries": lambda: str(random.randint(1, 5)),
    "isolated_count": lambda: str(random.randint(2, 50)),
    "switch": lambda: f"switch-{random.choice(['core','dist','access'])}-{random.randint(1,10)}",
    "flap_count": lambda: str(random.randint(5, 100)),
    "router": lambda: f"router-{random.choice(['core','border'])}-{random.randint(1,4)}",
    "backup": lambda: f"router-backup-{random.randint(1,2)}",
    # Linux VM
    "hostname": _rand_host,
    "sys": lambda: f"{random.randint(1,30)}",
    "idle_pct": lambda: f"{random.randint(5,80)}",
    "load_avg": lambda: f"{random.uniform(0.1,15):.2f}",
    "load_1m": lambda: f"{random.uniform(0.1,20):.2f}",
    "load_5m": lambda: f"{random.uniform(0.1,15):.2f}",
    "load_15m": lambda: f"{random.uniform(0.1,10):.2f}",
    "process": lambda: random.choice(["java", "python3", "node", "postgres", "redis-server", "nginx"]),
    "pid": lambda: str(random.randint(1000, 65535)),
    "nice": lambda: str(random.choice([0, -5, -10, 10, 19])),
    "job": lambda: random.choice(["backup.sh", "cleanup.py", "report-gen", "db-vacuum"]),
    "device": lambda: random.choice(["sda", "sdb", "nvme0n1"]),
    "read_mbps": lambda: f"{random.uniform(0.1, 500):.1f}",
    "write_mbps": lambda: f"{random.uniform(0.1, 300):.1f}",
    "swap_used": lambda: str(random.randint(0, 4096)),
    "swap_total": lambda: str(random.choice([4096, 8192, 16384])),
    "swap_pct": lambda: f"{random.randint(50,100)}",
    "zombie_count": lambda: str(random.randint(1, 20)),
    "pids": lambda: ', '.join([str(random.randint(1000, 65535)) for _ in range(random.randint(2, 5))]),
    "iowait": lambda: f"{random.uniform(10,80):.1f}",
    "max": lambda: str(random.choice([1024, 4096, 65535])),
    "freed": lambda: str(random.randint(50, 2000)),
    "temp": lambda: str(random.randint(70, 105)),
    "freq": lambda: str(random.randint(800, 3000)),
    "address": lambda: f"0x{_rand_hex(12)}",
    "library": lambda: random.choice(["libc.so.6", "libssl.so", "libpthread.so", "libjvm.so"]),
    "syscall": lambda: random.choice(["read", "write", "futex", "epoll_wait"]),
    "panic_msg": lambda: random.choice(["VFS: unable to mount root fs", "kernel BUG at mm/page_alloc.c",
                                        "unable to handle kernel paging request"]),
    "cached": lambda: str(random.randint(100, 8000)),
    # Linux Host
    "cpu_temp": lambda: str(random.randint(40, 105)),
    "fan_rpm": lambda: str(random.randint(800, 6000)),
    "ambient_temp": lambda: str(random.randint(18, 45)),
    "power": lambda: str(random.randint(100, 1500)),
    "inlet_temp": lambda: str(random.randint(15, 40)),
    "disk_temp": lambda: str(random.randint(25, 60)),
    "cpu_count": lambda: str(random.choice([1, 2, 4])),
    "cpu_model": lambda: random.choice(["Xeon E5-2680", "EPYC 7742", "Xeon Gold 6248"]),
    "memory_gb": lambda: str(random.choice([32, 64, 128, 256, 512])),
    "disk_count": lambda: str(random.randint(2, 24)),
    "bios_ver": lambda: f"v{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,9)}",
    "bmc_ver": lambda: f"v{random.randint(2,5)}.{random.randint(0,15)}",
    "cpld_ver": lambda: f"v{random.randint(1,3)}.{random.randint(0,5)}",
    "fan_id": lambda: f"FAN-{random.randint(1,8)}",
    "min_rpm": lambda: str(random.choice([1000, 1500, 2000])),
    "psu_id": lambda: f"PSU-{random.randint(1,4)}",
    "output": lambda: str(random.randint(200, 1200)),
    "rated": lambda: str(random.choice([750, 1000, 1200, 1600])),
    "eff": lambda: f"{random.randint(70,95)}",
    "dimm": lambda: f"DIMM-{random.choice('ABCDEFGH')}{random.randint(1,4)}",
    "sensor": lambda: random.choice(["CPU0_Temp", "SYS_Fan1", "VRM_Voltage", "PCH_Temp"]),
    "value": lambda: f"{random.uniform(50,120):.1f}",
    "countdown": lambda: str(random.choice([30, 60, 120])),
    "disk_a": lambda: "sda",
    "disk_b": lambda: "sdb",
}


class LogGenerator:
    """
    Generates realistic synthetic log entries for cloud infrastructure monitoring.
    Each log session is a sequence of log lines with timestamps, levels, and structured content.
    """

    def __init__(self, seed=None):
        self.rng = random.Random(seed)
        random.seed(seed)

    def _fill_template(self, template):
        """Fill a log template with random realistic values."""
        result = template
        # Find all placeholders
        import re
        placeholders = re.findall(r'\{(\w+)\}', template)
        for ph in placeholders:
            if ph in TEMPLATE_VARIABLES:
                result = result.replace('{' + ph + '}', str(TEMPLATE_VARIABLES[ph]()), 1)
            else:
                result = result.replace('{' + ph + '}', f"<{ph}>", 1)
        return result

    def _get_level_distribution(self, is_anomaly, severity=None):
        """Get log level distribution based on anomaly status.
        
        Distributions now OVERLAP significantly to create realistic classification
        difficulty. Normal systems DO produce errors, and anomalous systems still
        produce plenty of INFO logs.
        """
        # Add per-sample random variation (realistic: each system behaves differently)
        noise = lambda: self.rng.uniform(-0.06, 0.06)

        if not is_anomaly:
            # Normal systems still have errors! (noisy baseline)
            base = {"INFO": 0.58, "WARN": 0.22, "ERROR": 0.15, "CRITICAL": 0.05}
        elif severity == "critical":
            base = {"INFO": 0.38, "WARN": 0.24, "ERROR": 0.25, "CRITICAL": 0.13}
        elif severity == "high":
            base = {"INFO": 0.42, "WARN": 0.26, "ERROR": 0.22, "CRITICAL": 0.10}
        else:  # medium / low
            base = {"INFO": 0.48, "WARN": 0.25, "ERROR": 0.19, "CRITICAL": 0.08}

        # Apply per-sample noise to simulate real-world variability
        noisy = {k: max(0.01, v + noise()) for k, v in base.items()}
        total = sum(noisy.values())
        return {k: v / total for k, v in noisy.items()}

    def _pick_level(self, distribution, index, total, is_anomaly, anomaly_start_idx=None):
        """Pick a log level, shifting toward errors after anomaly start."""
        levels = list(distribution.keys())
        weights = list(distribution.values())

        if is_anomaly and anomaly_start_idx is not None and index >= anomaly_start_idx:
            # Increase error weight progressively after anomaly
            progress = min(1.0, (index - anomaly_start_idx) / max(1, total - anomaly_start_idx))
            weights[2] *= (1 + 2.0 * progress)  # ERROR
            weights[3] *= (1 + 3.0 * progress)  # CRITICAL
            weights[0] *= max(0.2, 1 - progress)  # INFO
            total_w = sum(weights)
            weights = [w / total_w for w in weights]

        return self.rng.choices(levels, weights=weights, k=1)[0]

    def generate_log_session(self, layer_key, num_lines=30, is_anomaly=False,
                             severity=None, anomaly_start_idx=None, base_time=None):
        """
        Generate a synthetic log session for a given infrastructure layer.

        Args:
            layer_key: Layer identifier (e.g., 'cdn', 'firewall', 'database')
            num_lines: Number of log lines to generate
            is_anomaly: Whether this is an anomalous session
            severity: Severity level for anomaly distribution
            anomaly_start_idx: Index where anomaly starts (for temporal alignment)
            base_time: Starting timestamp

        Returns:
            str: Complete log session as text
        """
        if base_time is None:
            base_time = datetime(2026, 3, 1) + timedelta(
                days=self.rng.randint(1, 30),
                hours=self.rng.randint(0, 23)
            )

        templates = LOG_TEMPLATES.get(layer_key, LOG_TEMPLATES["application"])
        level_dist = self._get_level_distribution(is_anomaly, severity)

        # Scale anomaly_start to log lines
        if anomaly_start_idx is not None:
            # Anomaly start is in metric indices (0-59), scale to log lines
            log_anomaly_start = int(anomaly_start_idx * num_lines / 60)
        else:
            log_anomaly_start = None

        lines = []
        for i in range(num_lines):
            timestamp = base_time + timedelta(seconds=i * self.rng.randint(1, 120))
            level = self._pick_level(level_dist, i, num_lines, is_anomaly, log_anomaly_start)

            # Pick a random template for this level
            level_templates = templates.get(level, templates.get("INFO", ["Unknown event"]))
            template = self.rng.choice(level_templates)
            content = self._fill_template(template)

            ts_str = timestamp.strftime("%Y-%m-%d %H:%M:%S.") + f"{self.rng.randint(0,999):03d}"
            log_line = f"{ts_str} [{level:<8s}] [{layer_key.upper():<12s}] {content}"
            lines.append(log_line)

        return "\n".join(lines)

    def generate_session_for_scenario(self, layer_key, scenario_config=None,
                                      num_lines=None, anomaly_start_idx=None, base_time=None):
        """
        Generate a log session aligned with a metric scenario.

        Args:
            layer_key: Layer key from anomaly_scenarios.yaml
            scenario_config: Scenario dict (None for normal)
            num_lines: Number of lines (random 20-60 if None)
            anomaly_start_idx: From metric generation
            base_time: Starting timestamp

        Returns:
            str: Log session text
        """
        if num_lines is None:
            num_lines = self.rng.randint(20, 60)

        if scenario_config is None:
            return self.generate_log_session(
                layer_key, num_lines=num_lines, is_anomaly=False, base_time=base_time
            )
        else:
            return self.generate_log_session(
                layer_key, num_lines=num_lines, is_anomaly=True,
                severity=scenario_config.get("severity", "medium"),
                anomaly_start_idx=anomaly_start_idx, base_time=base_time
            )
