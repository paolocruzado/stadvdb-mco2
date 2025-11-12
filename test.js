import { createTunnel } from 'tunnel-ssh';
import mysql from 'mysql2/promise';
import dotenv from 'dotenv';

dotenv.config();

async function runNode(node) {
  try {
    await createTunnel(
      { autoClose: true },
      { host: '127.0.0.1', port: node.localPort },
      node.ssh,
      { srcAddr: '127.0.0.1', srcPort: node.localPort, dstAddr: '127.0.0.1', dstPort: 3306 }
    );
    console.log(`✅ Tunnel open on localhost:${node.localPort}`);

    const db = await mysql.createPool(node.db);
    const [rows] = await db.query('SELECT COUNT(*) AS count FROM title_basics');
    console.log(`Rows in ${node.name}:`, rows[0].count);
  } catch (err) {
    console.error(`❌ Error for ${node.name}:`, err);
  }
}

async function run() {
  const nodes = [
    {
      name: 'Node1',
      ssh: { host: process.env.SSH1_HOST, port: Number(process.env.SSH1_PORT), username: process.env.SSH1_USER, password: process.env.SSH1_PASS },
      localPort: Number(process.env.DB1_PORT),
      db: { host: '127.0.0.1', port: Number(process.env.DB1_PORT), user: process.env.DB1_USER, password: process.env.DB1_PASS, database: process.env.DB1_NAME }
    },
    {
      name: 'Node2',
      ssh: { host: process.env.SSH2_HOST, port: Number(process.env.SSH2_PORT), username: process.env.SSH2_USER, password: process.env.SSH2_PASS },
      localPort: Number(process.env.DB2_PORT),
      db: { host: '127.0.0.1', port: Number(process.env.DB2_PORT), user: process.env.DB2_USER, password: process.env.DB2_PASS, database: process.env.DB2_NAME }
    }
  ];

  for (const node of nodes) {
    await runNode(node);
  }
}

run();
