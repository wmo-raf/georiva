# Sending files to a push feed

A push feed ([ADR-0034](adr/0034-push-feeds-replace-manual-upload.md)) takes files that a met service sends to it.
A machine sender sends them with `georiva_push.py`, the official sender script. It runs on Python 3.6 or later and
needs nothing beyond the standard library (it uses boto3 when that is installed).

## Setting up a sender

1. On the feed's page in GeoRiva, add a machine sender. The page shows its config block **once**. Save the block on
   the sending machine where only the sending account can read it:

   ```bash
   mkdir -p ~/.georiva && chmod 700 ~/.georiva
   $EDITOR ~/.georiva/sender.conf      # paste the block
   chmod 600 ~/.georiva/sender.conf
   ```

2. Download `georiva_push.py` from the same page. The instance serves the script itself, so the copy you run matches
   the server it sends to. Put it somewhere stable, such as `/opt/georiva/georiva_push.py`.

3. Check that the sending machine can reach the endpoint named in the block, then send one file by hand:

   ```bash
   python3 /opt/georiva/georiva_push.py send /data/georiva/wrf_20261027T06Z.nc --config ~/.georiva/sender.conf
   ```

   The file should then show as a Delivery on the feed's page.

## How the script decides what to send

- **Markers.** Once a file is sent, the script writes `<file>.sent` next to it, holding the file's checksum. A file
  whose marker still matches is not sent again. A file that has changed since it was sent goes again under the same
  name, and GeoRiva ingests it again. `send FILE --force` sends a file even when its marker matches.
- **Folder mode.** `send --dir DIR` makes one pass over a folder and sends, oldest first, every file whose marker does
  not match it. The pass leaves alone:
  - dot-files, names ending `.part` or `.tmp`, and the `.sent` markers;
  - files modified within the last `--settle` seconds (default 120);
  - files whose size or modification time changes over the 10 seconds the pass waits before it sends.

  Whatever writes into the folder should write each file under a `.part` (or `.tmp`) name and rename it once the
  file is complete. A rename is atomic, so a pass never sees half a file.
- **Retention.** `--keep N` removes sent files after the pass, oldest first, and keeps the newest N. A file without a
  matching marker is never removed, so a file that failed to send stays until a later pass sends it. Leave `--keep`
  off to keep everything.
- **Retries.** A send is tried five times over about a quarter of an hour on network errors and server errors. A 403
  is not retried: the credentials are wrong, the sender was revoked, or `georiva_dest` is not this sender's feed.
- **Overlapping runs.** A file is locked while it is sent, so two passes, or a pass and a single-file send, never send
  the same file at once. The lock uses `flock`; on Windows, or a filesystem that refuses `flock`, sends go unlocked.
  Keep the folder on a local disk where you can.
- **Exit status.** `0` when everything was sent or already sent, `1` when a file could not be sent, `2` for a usage or
  configuration error.

Run from cron, folder mode recovers from outages without anyone stepping in: whatever a pass could not send, the next
one sends.

## Scenario 1: the HPC can reach GeoRiva

The model's run script converts its output and sends it as soon as each file is ready. A cron job on the same machine
sends whatever the run script could not, for example during a network outage.

In the run script, after a file is converted:

```bash
OUT=/data/georiva
# write under a .part name, then rename, so a cron pass never picks up half a file
cdo -f nc4 copy "$RAW" "$OUT/wrf_${RUN}.nc.part" && mv "$OUT/wrf_${RUN}.nc.part" "$OUT/wrf_${RUN}.nc"
python3 /opt/georiva/georiva_push.py send "$OUT/wrf_${RUN}.nc" --config ~/.georiva/sender.conf \
  || echo "georiva_push failed; the cron pass will retry"
```

The crontab is the safety net:

```cron
*/10 * * * * python3 /opt/georiva/georiva_push.py send --dir /data/georiva --keep 40 --config ~/.georiva/sender.conf >> ~/.georiva/push.log 2>&1
```

Compute nodes on an HPC often have no outbound network, and some sites allow cron only on login nodes. Run both the
hook and the cron job where the network is, which is often a login or data-transfer node. On Slurm, `scrontab` can
stand in for cron.

## Scenario 2: the HPC copies to a middle server

The HPC cannot reach GeoRiva, but it can copy to a server that can. The HPC copies each file to a `.part` name and
renames it once the copy is complete:

```bash
scp "$OUT/wrf_${RUN}.nc" relay:/data/georiva/wrf_${RUN}.nc.part \
  && ssh relay mv /data/georiva/wrf_${RUN}.nc.part /data/georiva/wrf_${RUN}.nc
```

The middle server keeps the sender block and runs the cron job:

```cron
*/5 * * * * python3 /opt/georiva/georiva_push.py send --dir /data/georiva --keep 40 --config ~/.georiva/sender.conf >> ~/.georiva/push.log 2>&1
```

If a copy cannot be renamed into place (some upload tools cannot), the settle window and size check still keep a
pass from sending a file that is still arriving. Raise `--settle` if copies stall for longer than two minutes.

## Scenario 3: the files are already on a middle server

Files arrive on a server in their final format, from whatever already produces them. Only the cron job is needed:

```cron
*/5 * * * * python3 /opt/georiva/georiva_push.py send --dir /data/georiva --keep 40 --config ~/.georiva/sender.conf >> ~/.georiva/push.log 2>&1
```

Point `--dir` at the folder the files land in. If something else owns that folder and clears it, leave `--keep` off.

## Environment overrides

Any value in the block can come from the environment as `GEORIVA_PUSH_<KEY>`, for example
`GEORIVA_PUSH_SECRET_ACCESS_KEY`, so a secret can live in a secrets manager instead of the file. With every value in
the environment, `--config` can be left off.
