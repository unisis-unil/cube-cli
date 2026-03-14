# CLAUDE.md

## VCS — Jujutsu (jj)

Ce repo est géré avec Jujutsu (colocalisé git+jj). Ne JAMAIS utiliser
de commandes git directement.

### Workflow commit

1. `cargo fmt && cargo clippy -- -D warnings` avant chaque commit
2. Ne jamais ajouter de `Co-Authored-By` dans les messages de commit
3. Workflow :
   ```bash
   jj describe -m "type(scope): message"
   jj new
   jj bookmark set main -r @-
   jj git push
   ```

## Release

1. Bumper `version` dans `Cargo.toml`
2. `cargo check` pour mettre à jour `Cargo.lock`
3. `jj describe -m "release: vX.Y.Z"` puis `jj new` et `jj bookmark set main -r @-` et `jj git push`
4. `jj tag set vX.Y.Z -r main` puis `jj git push --tag 'glob:vX.Y.Z'`
