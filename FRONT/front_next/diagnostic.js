const fs = require('fs');
const os = require('os');
const path = require('path');

console.log('--- ENVIRONNEMENT ---');
console.log('OS:', os.platform(), os.release());
console.log('Node version:', process.version);
console.log('Current directory:', process.cwd());
console.log('Project directory:', __dirname);

console.log('\n--- FICHIERS ET DOSSIERS ---');
const dirs = fs.readdirSync(__dirname, { withFileTypes: true });
dirs.forEach(d => {
  console.log(d.isDirectory() ? '[DIR] ' + d.name : '      ' + d.name);
});

console.log('\n--- TEST ACCES ECRITURE ---');
try {
  fs.writeFileSync(path.join(__dirname, 'test_write.txt'), 'test');
  console.log('Ecriture OK');
  fs.unlinkSync(path.join(__dirname, 'test_write.txt'));
} catch (e) {
  console.error('Erreur écriture:', e);
} 