# Push feeds replace Manual Upload

> Amends [ADR 0003](0003-acquisition-model-fetchrun-uploadsession.md) (`UploadedFile` gives way to `Delivery`) and
> [ADR 0026](0026-a-model-run-is-a-first-class-ingestion-record.md) (a push feed closes a run as `declared_set`).

## Status

proposed

## Context

A national met service wants to send its WRF output to GeoRiva as each run finishes. The file is already a clean CF
NetCDF that the NetCDF path ingests, so nothing needs a plugin. What is missing is a way for files to be **sent** in
that GeoRiva can account for.

A `DataFeed` (a pull feed) gets a `FetchRun` per fetch, health chips, and a dashboard. A file written straight into the
`incoming` bucket gets none of that:

- nothing records who sent it. The chain starts at the Bucket notification as `system:bucket`;
- nothing notices when an expected run never arrives;
- a key that fails path checks is only a log line in the consumer;
- anyone with write access to `incoming` can write under any organisation's prefix, and the file is ingested into that
  organisation.

Manual Upload already covers the human half of "files are sent to us": `ManualUploadConfig`, `UploadSession`,
`UploadedFile`. Its `ManualUploadConfigVariable` rows are never read by ingestion, which routes by each Variable's
`sources`.

## Decision

1. **Every catalog has at most one feed, its single route in: a pull feed (`DataFeed`) or a push feed (`PushFeed`).**
   Saving either model refuses a catalog that already has the other. Nobody uploads into a pull-fed catalog; a pull
   feed fills its own gaps by fetching further back.
2. **`PushFeed` is its own model, not a `DataFeed` subclass.** Its schedule is an expectation to check, not a job for
   beat; there is nothing to fetch, retry or resume. It belongs to one catalog and describes one file shape:
   `is_forecast`, `valid_time_format`, `files_per_run` (default 1) and an optional `expected_interval`. A product that
   arrives in two shapes is two catalogs.
3. **Manual Upload is retired and becomes the push feed's human sender.** `ManualUploadConfig` gives way to `PushFeed`,
   `UploadedFile` to `Delivery`, and `ManualUploadConfigVariable` goes; routing stays on the Variables. `UploadSession`
   stays, as a person's batch of deliveries. Existing Manual Upload data is not migrated.
4. **A push feed has senders: machine senders and people.**
   - **Machine senders** use MinIO credentials that GeoRiva creates through the MinIO admin API. Their policy allows only
     `PutObject` on `incoming/<org>/<catalog>/*`, built from the feed's own catalog. The secret is shown once; GeoRiva
     keeps the access key. Rotate and Revoke are on the feed's page, and deleting the feed revokes its senders.
   - **People** upload on the feed's page. GeoRiva writes the object and records the delivery itself.
   - Managing machine senders needs `manage_push_senders`. The feed itself uses the same organisation-scoped model
     permissions as pull feeds.
5. **Machine senders upload over the S3 API only,** through an nginx route to MinIO, behind whatever TLS fronts the
   admin site. No FTP or SFTP. The feed page shows an `rclone` config built from `MINIO_PUBLIC_ENDPOINT`, and warns
   while that address is plain HTTP.
6. **A file in `incoming` is attributed by who wrote it.** The Bucket notification's `userIdentity.principalId` is
   matched against the feed's machine senders.
   - A person's upload is already recorded by the upload view, and is not recorded twice.
   - A file the Sweep finds has no principal. It is accepted, attributed by prefix and marked "found by sweep", which is
     safe because only the feed's senders and GeoRiva can write there.
   - A file written by anyone else (root or other admin credentials), or under a key that belongs to no push feed, is
     **rejected**.
7. **A `Delivery` is `received → accepted | rejected`.** An accepted delivery hands off to ingestion; its outcome stays
   on the linked `FileIngestion`. A rejected file moves to a **quarantine bucket** under the same key, where the Sweep
   never sees it, and expires after 30 days. **Retry** moves it back. Each rejection records a refused
   `DeliveryRejected` event: on the organisation's Failures panel, or among Unattributed events when the key's
   organisation does not exist. A delivery starts its own Correlation, as a fetch does.
8. **A push feed closes a run as `declared_set`** once `files_per_run` accepted deliveries for one reference time have
   ingested. Anything that does not add up falls back to `quiet_period`; a person's batch still closes through
   `upload_session`.
9. **Overdue is derived, and recorded once.** A feed with an `expected_interval` is overdue when its last accepted
   delivery is older than that interval; the chip is computed on read. A beat task records one failed
   `PushFeedOverdue` event per overdue spell, grouped under the feed on the Failures panel. No email or webhook.
10. **The Manual Upload setup wizard becomes "New push feed":** catalog, feed settings, Sample File, collections,
    review. Machine senders are added afterwards on the feed's page, never mid-wizard.
11. **The glossary and UI say pull feed and push feed. The code keeps `DataFeed`.**

## Consequences

- `incoming` is purely the push bucket. Pull feeds write to `sources` or `staging`, and nothing is ingested from
  `incoming` without a push feed and a known sender.
- Testing with root credentials no longer ingests anything: the file is quarantined, and you add a test sender instead.
- The consumer resolves a feed and a sender before ingestion. A rejection never reaches `FileIngestion`.
- GeoRiva now manages MinIO users and policies, not only bucket policies. That depends on silo's admin API accepting the
  `minio` client's `MinioAdmin`, which must be confirmed first.
- The FTP/SFTP flags considered for silo are not adopted.

## Alternatives considered

- **A plugin, and an FTP server, per sender.** The data already arrives in a format GeoRiva reads; the gap was
  accounting, not reading.
- **A `DataFeed` subclass with a no-op source.** It would reuse the list page and chips, but beat would schedule a feed
  that never fetches, and arrivals would pose as `FetchRun`s.
- **An "expected every N hours" flag on the collection only.** No record of who sent what, no home for credentials,
  and rejected files stay invisible.
- **Attribution by prefix alone.** Operators' uploads and stray admin writes would count as the sender's.
- **Named run slots** ("00Z due by 06:00"). They name the missing run exactly, but break when a sender shifts its run
  times.
- **Leaving rejected files in place, or deleting them.** The Sweep would revive them every pass; deleting loses the
  evidence and forces a resend for faults on GeoRiva's side.
- **SFTP alongside S3.** A public port and a host key to manage, and it is unconfirmed whether silo publishes an
  SFTP upload only once it completes.
