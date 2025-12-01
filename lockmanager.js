const RETRY_DELAY_MS = 200;
const MAX_RETRIES = 50; // Wait up to ~10 seconds

export class LockManager {
  constructor(db) {
    this.db = db;
  }

  async acquire(lockName, nodeName) {
    let attempts = 0;

    while (attempts < MAX_RETRIES) {
      try {
        await this.db.query(
          "INSERT INTO distributed_lock (lock_name, locked_by) VALUES (?, ?)",
          [lockName, nodeName]
        );
        return true;
      } catch (err) {
        if (err.code === 'ER_DUP_ENTRY') {
          attempts++;
          await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
        } else {
          throw err; 
        }
      }
    }
    throw new Error(`Could not acquire lock '${lockName}' after ${MAX_RETRIES} attempts.`);
  }
  
  async release(lockName) {
    try {
      await this.db.query("DELETE FROM distributed_lock WHERE lock_name = ?", [lockName]);
    } catch (err) {
      console.error(`Failed to release lock ${lockName}:`, err);
    }
  }
}