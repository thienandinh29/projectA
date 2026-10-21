# Moving collection to an always-on cloud server

Why: from 2026-10-19 to 2026-10-21 the PC collected under 5 of ~47 hours, in gaps of
21–22 h whenever it was switched off. A prospective study needs continuous collection.

This guide **moves the existing stack**: all three Docker volumes go to the server, so
the database, the collection ledgers and the Kafka offsets that define the prospective
boundary (`docs/collection-baseline-20260928T084655Z.json`) continue unchanged. The
move creates one disclosed collection gap. Starting a fresh broker or database on the
server instead would invalidate the baseline and begin a new cohort. Don't do that
without deciding it as a protocol change.

Local commands are PowerShell, run in the main project folder. Server commands are bash.
`SERVER` means the server's IP address.

## 1. Choose and create the server

- Ubuntu 24.04 LTS, **2 vCPU, 4 GB RAM, 40 GB disk** (Redpanda reserves 1 GB; the
  embedding model and DuckDB need the rest). Any mainstream VPS provider works;
  this size is roughly US$5–25/month depending on the provider.
- Region: anywhere outside the network that blocks Investing.com and GDELT here. Step 3
  tests that before anything is moved, so a cheap server can be deleted if it fails.
- Choose SSH-key login when creating it (step 2), not a password.

## 2. SSH key (local, once)

```powershell
ssh-keygen -t ed25519 -C "paper1-collection"
Get-Content $HOME\.ssh\id_ed25519.pub
```

Paste the printed public key into the provider's "SSH keys" field when creating the
server. Then connect: `ssh root@SERVER`.

## 3. Test the sources from the server first (5 min)

```bash
curl -sS -m 30 -o /dev/null -w "Investing.com %{http_code} %{time_total}s\n" -A "FinancialIngestionWorker/1.0" https://www.investing.com/rss/news_14.rss
curl -sS -m 30 "https://api.gdeltproject.org/api/v2/doc/doc?query=inflation&mode=ArtList&format=json&maxrecords=3" | head -c 200; echo
curl -sS -m 30 -o /dev/null -w "Guardian %{http_code}\n" https://www.theguardian.com/uk/business/rss
```

Investing.com should return `200` (not a 15 s failure), and GDELT should start with
`{"articles"`. If GDELT still fails it may be rate limiting, not blocking. Note it,
because the Friday GDELT decision uses it. If Investing.com fails too, try another region.

## 4. Prepare the server (as root, once)

```bash
apt-get update && apt-get -y upgrade
adduser --disabled-password --gecos "" paper1
mkdir -p /home/paper1/.ssh && cp ~/.ssh/authorized_keys /home/paper1/.ssh/
chown -R paper1:paper1 /home/paper1/.ssh && chmod 700 /home/paper1/.ssh

# Firewall: SSH only. (Docker bypasses ufw, which is why docker-compose.server.yml
# removes every published port.)
ufw allow OpenSSH && ufw --force enable

# Research timestamps depend on the clock: UTC and NTP must be active.
timedatectl set-timezone UTC
timedatectl            # expect "System clock synchronized: yes"

# 2 GB swap as a safety margin for the embedding model.
fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab

apt-get -y install unattended-upgrades
curl -fsSL https://get.docker.com | sh
usermod -aG docker paper1
```

From now on, log in as `ssh paper1@SERVER`.

## 5. Code on the server

The folder **must** be named `financial-ingestion-pipeline`. Compose derives the
project and volume names from it, and the restored volumes use those names.

```bash
git clone https://github.com/thienandinh29/projectA.git ~/financial-ingestion-pipeline
cd ~/financial-ingestion-pipeline
git log --oneline -1          # note the commit
echo "alias dc='docker compose -f docker-compose.yml -f docker-compose.server.yml'" >> ~/.bashrc
source ~/.bashrc
docker volume create ingestion-data
dc create redpanda redis      # creates the Compose volumes empty; starts nothing
```

## 6. Stop and package the PC's data (local)

This is the protocol's normal stop: collectors first, drain with the writer still
running, and stop the writer only after the drain succeeds. Write down the time of the
collector stop (UTC); it is the start of the migration gap.

```powershell
docker compose --profile workers stop worker-rss worker-gdelt worker-sec
if ($LASTEXITCODE -ne 0) { throw 'Collector stop failed' }
docker compose --profile tools run --rm --no-deps research-tools -m scripts.drain_delivery --timeout 120 --baseline-path /app/docs/collection-baseline-20260928T084655Z.json --output /app/data/research/migration-drain-20261021.json
if ($LASTEXITCODE -ne 0) { throw 'Drain failed: keep the writer running and investigate' }
docker compose --profile workers stop lakehouse-writer
docker compose stop console redpanda redis

mkdir migration; cd migration
foreach ($v in 'ingestion-data','financial-ingestion-pipeline_redpanda-data','financial-ingestion-pipeline_redis-data') {
  docker run --rm -v "${v}:/from:ro" -v "${PWD}:/to" alpine:3.20 tar --numeric-owner -C /from -czf "/to/$v.tgz" .
}
Get-FileHash *.tgz -Algorithm SHA256 | Format-Table Hash, Path
scp *.tgz paper1@SERVER:~/
```

Keep the PC's volumes untouched afterwards: they are the pre-migration backup.
**Never start the collectors or writer on the PC again** while the server collects.
Two collectors would split the record into two databases.

## 7. Restore and start on the server

```bash
cd ~
sha256sum *.tgz               # must equal the PC's hashes
for v in ingestion-data financial-ingestion-pipeline_redpanda-data financial-ingestion-pipeline_redis-data; do
  docker run --rm -v "$v:/to" -v "$HOME:/from:ro" alpine:3.20 tar --numeric-owner -xzpf "/from/$v.tgz" -C /to
done

cd ~/financial-ingestion-pipeline
export COLLECTION_CODE_REVISION=$(git rev-parse HEAD)
export COLLECTION_WORKTREE_STATE=$( [ -z "$(git status --porcelain)" ] && echo clean || echo dirty )
echo "$COLLECTION_CODE_REVISION $COLLECTION_WORKTREE_STATE"    # must say clean
dc --profile workers --profile tools build lakehouse-writer worker-rss worker-gdelt research-tools
dc up -d redpanda redis console
dc --profile workers up -d lakehouse-writer worker-rss worker-gdelt
date -u                       # end of the migration gap
```

Check that the existing data came across before trusting it:

```bash
dc --profile tools run --rm --no-deps research-tools -m scripts.inspect_collection_sessions | head -40
dc --profile workers logs --tail 30 lakehouse-writer   # batch_committed, no fault halt
```

The old sessions appear as interrupted, and new sessions start. If the writer halts on a
transport fault or an offset problem, **stop the collectors and investigate**; do not
reset offsets.

## 8. Record the move

Add one dated line to `docs/research-protocol.md`, next to the panel v2 entry: collector
stop time and server start time (the gap), server provider/region, the commit, and the
drain report path. Commit and push it from the PC.
`RESEARCH_DATABASE_ID` stays the same because the volume and its contents are the same;
the protocol line discloses the host change.

## 9. Daily routine

```bash
ssh paper1@SERVER
cd ~/financial-ingestion-pipeline
dc --profile tools run --rm --no-deps research-tools -m scripts.collection_health --hours 24
```

- Code changes: push from the PC, then on the server run `git pull`, export the two
  variables (step 7), and rebuild and restart **only** the collectors and writer.
- Review work (eligibility and sentiment templates) runs on the server with the same
  commands as before, using `dc` instead of `docker compose`. Copy small files to and
  from the server with `scp`.
- Redpanda console: run `ssh -L 8080:localhost:8080 paper1@SERVER`, then open http://localhost:8080 on the PC.

## 10. Weekly backup to the PC

The server volume becomes the only live copy of the research data. Once a week:

```bash
docker run --rm -v ingestion-data:/from:ro -v "$HOME:/to" alpine:3.20 tar --numeric-owner -C /from -czf /to/ingestion-backup-$(date -u +%Y%m%d).tgz .
```

Then run this on the PC: `scp paper1@SERVER:~/ingestion-backup-*.tgz D:\paper1-backups\`.
Use any folder outside the project. Delete old backups on the server afterwards.
The live DuckDB file is being written while the backup runs. For a fully consistent
backup, take it during a protocol stop (step 6), as you would for a snapshot.
