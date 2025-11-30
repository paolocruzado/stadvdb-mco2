import { v4 as uuidv4 } from "uuid";

export async function runTransaction(db, isolation, operations, nodeName, replicator) {
  const conn = await db.getConnection();
  const logs = [];

  try {
    await conn.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolation}`);
    logs.push(`[${nodeName}] Isolation = ${isolation}`);

    await conn.beginTransaction();
    logs.push(`[${nodeName}] BEGIN`);

    let lastWriteSql = null;

    for (const op of operations) {
      const result = await op(conn);

      if (result?.writeSql) lastWriteSql = result.writeSql;

      logs.push(result.msg || result);
    }

    await conn.commit();
    logs.push(`[${nodeName}] COMMIT`);

    if (lastWriteSql && replicator) {
      const replicationId = uuidv4(); 
      try {
        const replicationResult = await replicator.runReplication(
          nodeName,
          lastWriteSql,
          replicationId
        );
        logs.push(`[${nodeName}] Replication attempted: ${JSON.stringify(replicationResult)}`);
      } catch (repErr) {
        logs.push(`[${nodeName}] Replication failed: ${repErr.message}`);
      }
    }

    return { success: true, logs };
  } catch (err) {
    logs.push(`[${nodeName}] ERROR: ${err.message}`);
    await conn.rollback();
    logs.push(`[${nodeName}] ROLLBACK`);
    return { success: false, logs };
  } finally {
    conn.release();
  }
}
