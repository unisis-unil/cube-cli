# CLAUDE.md

## Commit

1. `cargo fmt && cargo clippy -- -D warnings` avant chaque commit
2. Ne jamais ajouter de `Co-Authored-By` dans les messages de commit

## Release

1. Bumper `version` dans `Cargo.toml`
2. `cargo check` pour mettre à jour `Cargo.lock`
3. Commit avec message `release: vX.Y.Z`
4. Le tag git doit être poussé séparément (`git tag vX.Y.Z && git push origin vX.Y.Z`) car `jj git push` ne pousse pas les tags
