import { and, lt, eq, sql, inArray } from "drizzle-orm";
import type { Db } from "@paperclipai/db";
import { heartbeatRuns, heartbeatRunEvents } from "@paperclipai/db";
import { logger } from "../middleware/logger.js";

/**
 * Retention policy for heartbeat_runs:
 *
 * - After NULLIFY_DAYS, null out large blob columns (result_json, stdout_excerpt,
 *   stderr_excerpt, context_snapshot) on finished runs. This reclaims the bulk of
 *   storage while keeping the run metadata for audit/history.
 *
 * - After DELETE_DAYS, delete the run rows entirely (and their events).
 */

/** Days after which large blob columns are nullified on finished runs. */
const DEFAULT_NULLIFY_DAYS = 7;

/** Days after which finished runs (and their events) are hard-deleted. */
const DEFAULT_DELETE_DAYS = 30;

/** Maximum rows to process per batch to avoid long-running transactions. */
const BATCH_SIZE = 1_000;

/** Maximum number of batches per sweep. */
const MAX_ITERATIONS = 200;

/**
 * Nullify large blob columns on finished heartbeat runs older than `nullifyDays`.
 *
 * Only targets runs that still have at least one non-null blob column, so
 * re-running is idempotent and cheap once caught up.
 */
async function nullifyOldRunBlobs(
  db: Db,
  nullifyDays: number,
): Promise<number> {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - nullifyDays);

  let totalUpdated = 0;
  let iterations = 0;

  while (iterations < MAX_ITERATIONS) {
    // Find a batch of old finished runs that still have blob data
    const batch = await db
      .select({ id: heartbeatRuns.id })
      .from(heartbeatRuns)
      .where(
        and(
          lt(heartbeatRuns.finishedAt, cutoff),
          inArray(heartbeatRuns.status, ["completed", "failed", "crashed", "cancelled", "timeout"]),
          sql`(
            ${heartbeatRuns.resultJson} IS NOT NULL
            OR ${heartbeatRuns.stdoutExcerpt} IS NOT NULL
            OR ${heartbeatRuns.stderrExcerpt} IS NOT NULL
            OR ${heartbeatRuns.contextSnapshot} IS NOT NULL
          )`,
        ),
      )
      .limit(BATCH_SIZE);

    if (batch.length === 0) break;

    const ids = batch.map((r) => r.id);
    await db
      .update(heartbeatRuns)
      .set({
        resultJson: null,
        stdoutExcerpt: null,
        stderrExcerpt: null,
        contextSnapshot: null,
        updatedAt: new Date(),
      })
      .where(inArray(heartbeatRuns.id, ids));

    totalUpdated += ids.length;
    iterations++;

    if (batch.length < BATCH_SIZE) break;
  }

  if (iterations >= MAX_ITERATIONS) {
    logger.warn(
      { totalUpdated, iterations, cutoffDate: cutoff },
      "Heartbeat run blob nullification hit iteration limit; some rows may remain",
    );
  }

  if (totalUpdated > 0) {
    logger.info({ totalUpdated, nullifyDays }, "Nullified blob columns on old heartbeat runs");
  }

  return totalUpdated;
}

/**
 * Delete heartbeat runs (and their events) older than `deleteDays`.
 *
 * Events are deleted first to satisfy foreign key constraints.
 */
async function deleteOldRuns(
  db: Db,
  deleteDays: number,
): Promise<{ runsDeleted: number; eventsDeleted: number }> {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - deleteDays);

  let totalRunsDeleted = 0;
  let totalEventsDeleted = 0;
  let iterations = 0;

  while (iterations < MAX_ITERATIONS) {
    // Find a batch of old finished runs to delete
    const batch = await db
      .select({ id: heartbeatRuns.id })
      .from(heartbeatRuns)
      .where(
        and(
          lt(heartbeatRuns.finishedAt, cutoff),
          inArray(heartbeatRuns.status, ["completed", "failed", "crashed", "cancelled", "timeout"]),
        ),
      )
      .limit(BATCH_SIZE);

    if (batch.length === 0) break;

    const ids = batch.map((r) => r.id);

    // Delete events first (FK constraint)
    const eventsDeleted = await db
      .delete(heartbeatRunEvents)
      .where(inArray(heartbeatRunEvents.runId, ids))
      .returning({ id: heartbeatRunEvents.id })
      .then((rows) => rows.length);

    // Delete runs
    const runsDeleted = await db
      .delete(heartbeatRuns)
      .where(inArray(heartbeatRuns.id, ids))
      .returning({ id: heartbeatRuns.id })
      .then((rows) => rows.length);

    totalEventsDeleted += eventsDeleted;
    totalRunsDeleted += runsDeleted;
    iterations++;

    if (batch.length < BATCH_SIZE) break;
  }

  if (iterations >= MAX_ITERATIONS) {
    logger.warn(
      { totalRunsDeleted, totalEventsDeleted, iterations, cutoffDate: cutoff },
      "Heartbeat run deletion hit iteration limit; some rows may remain",
    );
  }

  if (totalRunsDeleted > 0) {
    logger.info(
      { runsDeleted: totalRunsDeleted, eventsDeleted: totalEventsDeleted, deleteDays },
      "Deleted old heartbeat runs and events",
    );
  }

  return { runsDeleted: totalRunsDeleted, eventsDeleted: totalEventsDeleted };
}

/**
 * Run a full retention sweep: nullify blobs on old runs, then delete very old runs.
 */
export async function pruneHeartbeatRuns(
  db: Db,
  nullifyDays: number = DEFAULT_NULLIFY_DAYS,
  deleteDays: number = DEFAULT_DELETE_DAYS,
): Promise<{ blobsNullified: number; runsDeleted: number; eventsDeleted: number }> {
  const blobsNullified = await nullifyOldRunBlobs(db, nullifyDays);
  const { runsDeleted, eventsDeleted } = await deleteOldRuns(db, deleteDays);
  return { blobsNullified, runsDeleted, eventsDeleted };
}

/**
 * Start a periodic heartbeat run retention interval.
 *
 * @param db - Database connection
 * @param intervalMs - How often to run (default: 1 hour)
 * @param nullifyDays - Days after which blob columns are nullified (default: 7)
 * @param deleteDays - Days after which runs are deleted entirely (default: 30)
 * @returns A cleanup function that stops the interval
 */
export function startHeartbeatRunRetention(
  db: Db,
  intervalMs: number = 60 * 60 * 1_000,
  nullifyDays: number = DEFAULT_NULLIFY_DAYS,
  deleteDays: number = DEFAULT_DELETE_DAYS,
): () => void {
  const timer = setInterval(() => {
    pruneHeartbeatRuns(db, nullifyDays, deleteDays).catch((err) => {
      logger.warn({ err }, "Heartbeat run retention sweep failed");
    });
  }, intervalMs);

  // Run once immediately on startup
  pruneHeartbeatRuns(db, nullifyDays, deleteDays).catch((err) => {
    logger.warn({ err }, "Initial heartbeat run retention sweep failed");
  });

  return () => clearInterval(timer);
}
