import { v4 as uuidv4 } from "uuid";

/**
 * Runs a set of SQL operations within a single transaction.
 * * @param {Object} db - The database pool/connection factory.
 * @param {string} isolation - The isolation level (e.g., 'READ COMMITTED').
 * @param {Array} operations - Array of objects: { sql: string, type: 'READ'|'WRITE', logMsg: string }
 * @param {string} nodeName - Label for logging.
 * @param {Object} replicator - The replication service.
 */
export async function runTransaction(db, isolation, operations, nodeName, replicator) {
  const conn = await db.getConnection();
  const logs = [];

  try {
    // 1. Set Isolation
    await conn.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolation}`);
    logs.push(`[${nodeName}] Isolation = ${isolation}`);
    console.log(`[${nodeName}] Isolation = ${isolation}`);

    // 2. Start Transaction
    await conn.beginTransaction();
    logs.push(`[${nodeName}] BEGIN`);

    let lastWriteSql = null;
    let queryResult = null;

    // 3. Execute Operations DIRECTLY inside the transaction scope
    for (const op of operations) {
      
      // We execute the query on 'conn' directly here. 
      // This guarantees it is inside the transaction.
      const [rows, fields] = await conn.query(op.sql);

      // Store the result for data extraction
      queryResult = rows;

      // Logging logic
      if (op.type === 'READ') {
        logs.push(`[${nodeName} READ] ${op.logMsg || ''} rows=${JSON.stringify(rows)}`);
        // Also push structured object so routes can extract data
        logs.push({ rows });
      } else {
        logs.push(`[${nodeName} WRITE] ${op.logMsg || 'executed'}`);
      }

      // Capture Write SQL for replication
      // We assume if type is WRITE, this is the query to replicate
      if (op.type === 'WRITE') {
        lastWriteSql = op.sql;
      }
    }

    // 4. Commit
    await conn.commit();
    logs.push(`[${nodeName}] COMMIT`);

    // 5. Replication (Must happen AFTER commit to ensure data exists)
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
    
    // Rollback on error
    await conn.rollback();
    logs.push(`[${nodeName}] ROLLBACK`);
    
    return { success: false, logs };
  } finally {
    conn.release();
  }
}