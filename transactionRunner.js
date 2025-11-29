import { v4 as uuidv4 } from "uuid";

export async function runTransaction(db, isolation, operations, nodeName, replicator) {
  const conn = await db.getConnection();
  const logs = [];

  try {
    // Set transaction isolation
    await conn.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolation}`);
    logs.push(`[${nodeName}] Isolation = ${isolation}`);

    // Begin transaction
    await conn.beginTransaction();
    logs.push(`[${nodeName}] BEGIN`);

    let lastWriteSql = null;

    // Execute all operations
    for (const op of operations) {
      const result = await op(conn);

      // Capture last write SQL for replication
      if (result?.writeSql) {
        lastWriteSql = result.writeSql;
      }

      logs.push(result.msg || result);
    }

    // Commit transaction
    await conn.commit();
    logs.push(`[${nodeName}] COMMIT`);

    // If a write occurred, replicate via Node1
    if (lastWriteSql && replicator) {
      const replicationId = uuidv4();

      // Insert into Node1 replication_log, not local node
      await replicator.dbMap.node1.query(
        `INSERT INTO replication_log 
          (id, origin_node, target_node, sql_text, status, attempts) 
         VALUES (?, ?, ?, ?, ?, ?)`,
        [replicationId, nodeName, "ALL_TARGETS", lastWriteSql, "pending", 0]
      );

      // Attempt replication immediately
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
