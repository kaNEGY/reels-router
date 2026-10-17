import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { checkSubject, getPrSubjects } from './check-commits.mjs';

test('разрешённые типы, необязательный scope, русский текст и граница 10/11', () => {
  for (const type of ['feat', 'fix', 'docs', 'data', 'test', 'ci', 'refactor', 'chore']) {
    assert.equal(checkSubject(`${type}: обновление данных`), null);
    assert.equal(checkSubject(`${type}(asr): обновление данных`), null);
  }
  assert.equal(checkSubject('feat: абвгдежзийк'), null);
  assert.ok(checkSubject('feat: абвгдежзий'));
  assert.ok(checkSubject('feat: абвгдежзий   '));
  assert.ok(checkSubject('feat: 😀😀😀😀😀😀'));
});

test('отклоняет неверный формат и пустой scope', () => {
  for (const subject of [
    'обновление документации', 'hello: обновление документации',
    'Feat: обновление документации', 'feat/asr: обновление документации',
    'feat(): обновление документации', 'feat(as r): обновление документации',
    'feat(asr: обновление документации', 'feat:обновление документации',
    'feat:  обновление документации', 'feat:           ',
    'feat: обновление\nдокументации', 'feat: обновление документации\n',
  ]) assert.ok(checkSubject(subject), subject);
});

test('CLI возвращает ненулевой exit code при ошибке', () => {
  const script = fileURLToPath(new URL('./check-commits.mjs', import.meta.url));
  for (const [message, status] of [['docs: обновление документации', 0], ['fix: коротко', 1]]) {
    assert.equal(spawnSync(process.execPath, [script, '--message', message]).status, status);
  }
});

test('PR: проверяет все новые коммиты и заголовок, исключает main и merge-коммиты', () => {
  const cwd = mkdtempSync(join(tmpdir(), 'reels-commit-check-'));
  const git = (...args) => execFileSync('git', args, { cwd, encoding: 'utf8' }).trim();
  try {
    git('init', '--initial-branch=main');
    git('config', 'user.name', 'Test');
    git('config', 'user.email', 'test@example.invalid');
    git('config', 'commit.gpgsign', 'false');
    git('commit', '--allow-empty', '-m', 'old invalid subject');
    git('checkout', '-b', 'feature');
    git('commit', '--allow-empty', '-m', 'bad subject');
    git('commit', '--allow-empty', '-m', 'feat(asr): добавление транскрибации');
    git('checkout', 'main');
    git('commit', '--allow-empty', '-m', 'another old invalid subject');
    const base = git('rev-parse', 'HEAD');
    git('checkout', 'feature');
    git('merge', '--no-ff', 'main', '-m', 'Merge main into feature');
    const pr = { title: 'ci: добавление проверки коммитов', base: { sha: base }, head: { sha: git('rev-parse', 'HEAD') } };
    const subjects = getPrSubjects(pr, cwd);
    assert.deepEqual(subjects.map(({ subject }) => subject), [pr.title, 'bad subject', 'feat(asr): добавление транскрибации']);
    assert.equal(subjects.filter(({ subject }) => checkSubject(subject)).length, 1);

    const eventPath = join(cwd, 'event.json');
    writeFileSync(eventPath, JSON.stringify({ pull_request: pr }));
    const script = fileURLToPath(new URL('./check-commits.mjs', import.meta.url));
    const result = spawnSync(process.execPath, [script, '--pr'], {
      cwd, env: { ...process.env, GITHUB_EVENT_PATH: eventPath }, encoding: 'utf8',
    });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /bad subject/u);
    assert.throws(() => getPrSubjects({ ...pr, head: { sha: '--all' } }, cwd));
  } finally {
    assert.equal(dirname(resolve(cwd)), resolve(tmpdir()));
    assert.ok(basename(cwd).startsWith('reels-commit-check-'));
    rmSync(cwd, { recursive: true, force: true });
  }
});
