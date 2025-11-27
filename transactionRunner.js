export async function runTransaction(db, isolation, operations, nodeName) {
  const conn = await db.getConnection();
  const logs = [];

  try {
    await conn.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolation}`);
    logs.push(`[${nodeName}] Isolation = ${isolation}`);
    
    await conn.beginTransaction();
    logs.push(`[${nodeName}] BEGIN`);

    for (const op of operations) {
      const msg = await op(conn);
      logs.push(msg);
    }

    await conn.commit();
    logs.push(`[${nodeName}] COMMIT`);

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
