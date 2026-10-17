import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

const format = /^(feat|fix|docs|data|test|ci|refactor|chore)(?:\([^()\s]+\))?: (\S[^\r\n]*)$/u;
const help = 'Ожидается type(scope): описание. Тип: feat, fix, docs, data, test, ci, refactor, chore; scope необязателен; описание — минимум 11 символов.';

export function checkSubject(subject) {
  const match = format.exec(subject);
  if (!match || match[0] !== subject) return help;
  if ([...match[2].trim()].length <= 10) {
    return 'Описание после двоеточия должно содержать минимум 11 символов без пробелов по краям.';
  }
  return null;
}

export function getPrSubjects(pr, cwd = process.cwd()) {
  // SHA берём из события, а не из имён веток; shell не используется.
  const base = pr?.base?.sha;
  const head = pr?.head?.sha;
  if (![base, head].every((sha) => typeof sha === 'string' && /^[a-f0-9]{40}$/u.test(sha))) {
    throw new Error('В событии PR отсутствуют корректные base/head SHA.');
  }
  if (typeof pr.title !== 'string') throw new Error('В событии PR отсутствует заголовок.');
  const git = (...args) => execFileSync('git', args, { cwd, encoding: 'utf8' });
  // Исключаем историю целевой ветки и коммиты с несколькими родителями.
  const commits = git('rev-list', '--reverse', '--no-merges', `${base}..${head}`)
    .trim().split('\n').filter(Boolean);
  return [
    { label: 'PR title', subject: pr.title },
    ...commits.map((sha) => ({
      label: sha.slice(0, 7),
      subject: git('show', '-s', '--format=%B', sha).split(/\r?\n/u)[0],
    })),
  ];
}

function main() {
  const args = process.argv.slice(2);
  let subjects;
  if (args.length === 2 && args[0] === '--message') {
    subjects = [{ label: 'message', subject: args[1] }];
  } else if (args.length === 1 && args[0] === '--pr') {
    const event = JSON.parse(readFileSync(process.env.GITHUB_EVENT_PATH, 'utf8'));
    subjects = getPrSubjects(event.pull_request);
  } else {
    throw new Error('Использование: node scripts/check-commits.mjs --message "type: описание" или --pr в GitHub Actions.');
  }
  for (const { label, subject } of subjects) {
    const error = checkSubject(subject);
    if (error) {
      // JSON.stringify оставляет пользовательский текст одной строкой журнала.
      console.error(`${label}: ${JSON.stringify(subject)} — ${error}`);
      process.exitCode = 1;
    }
  }
  if (!process.exitCode) console.log(`Проверено заголовков: ${subjects.length}. Ошибок нет.`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main();
  } catch (error) {
    console.error(`Проверка не выполнена: ${JSON.stringify(error.message)}`);
    process.exitCode = 1;
  }
}
