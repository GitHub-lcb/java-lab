import { mkdirSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const root = dirname(fileURLToPath(import.meta.url));
const mode = process.argv[2];
if (!['test', 'start'].includes(mode)) throw new Error('Usage: node server/java-runner.mjs <test|start>');
const lookup = spawnSync(process.platform === 'win32' ? 'where.exe' : 'which', ['javac'], { encoding: 'utf8' });
if (lookup.status !== 0) throw new Error('javac was not found. Install JDK 8 or newer and add it to PATH.');
const javac = lookup.stdout.trim().split(/\r?\n/)[0];
const java = join(dirname(javac), process.platform === 'win32' ? 'java.exe' : 'java');
const sourceRoot = join(root, 'src', 'main', 'java');
const output = join(root, 'build', mode);
const files = [];
function collect(directory) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) collect(path);
    else if (entry.name.endsWith('.java')) files.push(path);
  }
}
collect(sourceRoot);
if (mode === 'test') collect(join(root, 'src', 'test', 'java'));
mkdirSync(output, { recursive: true });
const compile = spawnSync(javac, ['-encoding', 'UTF-8', '-d', output, ...files], { stdio: 'inherit' });
if (compile.error) throw compile.error;
if (compile.status !== 0) process.exit(compile.status ?? 1);
const mainClasses = mode === 'test' ? ['lab.redis.CommandPolicyTest', 'lab.jvm.JvmProbeTest'] : ['lab.redis.RedisLabServer'];
for (const mainClass of mainClasses) {
  const run = spawnSync(java, ['-cp', output, mainClass], { stdio: 'inherit' });
  if (run.error) throw run.error;
  if (run.status !== 0) process.exit(run.status ?? 1);
}
