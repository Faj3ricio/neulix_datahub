import os
import re
from git import Repo
from datetime import date

# 1) Configurações iniciais
REPO = Repo(os.getcwd())
PYPROJECT_FILE = "pyproject.toml"
CHANGELOG_FILE = "CHANGELOG.md"

# 2) Convenções de commit para versionamento
CONVENTIONAL_TYPES = {
    'major': ['major'],
    'minor': ['feat', 'feature'],
    'patch': ['fix', 'perf', 'refactor', 'style', 'chore', 'build', 'ci', 'test']
}

def get_last_tag(repo):
    tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)
    return tags[-1].name if tags else None

def bump_version():
    last_tag = get_last_tag(REPO)
    if last_tag:
        major, minor, patch = map(int, last_tag.lstrip('v').split('.'))
        commits = list(REPO.iter_commits(f'{last_tag}..HEAD'))
    else:
        major, minor, patch = 0, 0, 0
        commits = list(REPO.iter_commits('HEAD'))

    bump = 'patch'
    for c in commits:
        msg = c.message.lower()
        for level, keywords in CONVENTIONAL_TYPES.items():
            if any(f'[{kw}]' in msg for kw in keywords):
                bump = level
                break
        if bump == 'major':
            break

    if bump == 'major':
        major += 1; minor = 0; patch = 0
    elif bump == 'minor':
        minor += 1; patch = 0
    else:
        patch += 1

    new_tag = f'v{major}.{minor}.{patch}'
    print(f"Bumping version ({bump}): {last_tag or 'nenhuma'} → {new_tag}")
    REPO.create_tag(new_tag, message=f"Release {new_tag}")
    return last_tag, new_tag

def update_pyproject_version(new_tag):
    version = new_tag.lstrip('v')
    if os.path.exists(PYPROJECT_FILE):
        content = open(PYPROJECT_FILE, 'r', encoding='utf-8').read()
        new_content = re.sub(r'version\s*=\s*"[^"]+"', f'version = "{version}"', content)
        with open(PYPROJECT_FILE, 'w', encoding='utf-8') as f:
            f.write(new_content)
        REPO.index.add([PYPROJECT_FILE])
        print(f"pyproject.toml atualizado para versão {version}.")
    else:
        print("pyproject.toml não encontrado, pulando atualização.")

def generate_changelog(last_tag, new_tag, reset=False):
    if reset and os.path.exists(CHANGELOG_FILE):
        os.remove(CHANGELOG_FILE)
        print("CHANGELOG.md removido para reiniciar do zero.")

    if last_tag:
        commits = list(REPO.iter_commits(f'{last_tag}..HEAD'))
    else:
        commits = list(REPO.iter_commits('HEAD'))
    if not commits:
        print("Nenhum commit novo para gerar changelog.")
        return

    grouped = {'major': [], 'minor': [], 'patch': []}
    for c in commits:
        msg = c.message.strip().splitlines()[0]
        low = msg.lower()
        level = 'patch'
        for lvl, kws in CONVENTIONAL_TYPES.items():
            if any(f'[{kw}]' in low for kw in kws):
                level = lvl
                break
        grouped[level].append(msg)

    lines = ["# Changelog", f"\n## {new_tag} – {date.today()}\n"]
    if grouped['major']:
        lines.append("### 🚀 Major Changes")
        lines += [f"- {m}" for m in grouped['major']]
        lines.append("")
    if grouped['minor']:
        lines.append("### ✨ Features")
        lines += [f"- {m}" for m in grouped['minor']]
        lines.append("")
    if grouped['patch']:
        lines.append("### 🐛 Fixes & Others")
        lines += [f"- {m}" for m in grouped['patch']]
        lines.append("")

    old = open(CHANGELOG_FILE, 'r', encoding='utf-8').read() if os.path.exists(CHANGELOG_FILE) else ''
    content = "\n".join(lines) + "\n" + old
    with open(CHANGELOG_FILE, 'w', encoding='utf-8') as f:
        f.write(content)

    REPO.index.add([CHANGELOG_FILE])
    print(f"CHANGELOG.md atualizado com {len(commits)} entradas (reset={reset}).")

def push_to_remote(new_tag):
    origin = REPO.remote('origin')
    branch = REPO.active_branch.name
    origin.push(f"HEAD:{branch}")
    origin.push(new_tag)
    print("Pushed branch e tag para o remote padrão (ex: GitHub)")

if __name__ == "__main__":
    last_tag, new_tag = bump_version()
    update_pyproject_version(new_tag)
    generate_changelog(last_tag, new_tag, reset=False)
    push_to_remote(new_tag)
    print("🎉 Release completa:", new_tag)
