# cube — UNISIS S3 Cubes CLI

CLI pour interroger les cubes SQLite de la plateforme **UNISIS S3** (Statistiques en Self-Service) de l'Université de Lausanne.

## Fonctionnalités

- **Catalogue et exploration** — Parcours progressif des cubes disponibles, avec recherche par regex insensible aux accents
- **Requêtes structurées** — Agrégation, filtrage, tri et limitation via des flags typés (pas besoin d'écrire du SQL)
- **SQL brut** — Pour les requêtes complexes impossibles à exprimer avec les flags
- **Synchronisation** — Téléchargement incrémental depuis Google Cloud Storage avec vérification CRC32C et intégrité SQLite

## Prérequis

- [Rust](https://rustup.rs/) (édition 2021+)
- [just](https://github.com/casey/just) — task runner
- [Google Cloud SDK](https://cloud.google.com/sdk) (`gcloud`) — pour la synchronisation

Avant la première synchronisation :

```bash
gcloud auth application-default login
```

## Installation

```bash
just install      # Compile et installe cube dans ~/.cargo/bin/
```

Le binaire `cube` sera installé dans `~/.cargo/bin/`. Assurez-vous que ce répertoire est dans votre `PATH` :

```bash
# Dans ~/.zshrc ou ~/.bashrc
export PATH="$HOME/.cargo/bin:$PATH"
```

## Tâches disponibles

```bash
just              # Affiche toutes les tâches disponibles
just build        # Compile en mode debug
just release      # Compile en mode release optimisé
just install      # Installe cube dans ~/.cargo/bin/
just uninstall    # Désinstalle cube
just test         # Lance tous les tests
just check        # Vérifie le code (clippy + fmt)
just fmt          # Formate le code
just sync         # Synchronise les cubes depuis GCS
just schema       # Liste les cubes disponibles
just run <ARGS>   # Exécute cube avec des arguments arbitraires
```

## Utilisation

### Explorer les cubes

```bash
cube schema                              # Catalogue compact de tous les cubes
cube schema --search "réussite"          # Filtrer par nom ou description (regex)
cube schema --search "réussite|cohorte"  # Plusieurs termes
cube schema infrastructures_surface      # Dimensions d'un cube
cube schema infrastructures_surface "Faculté"  # Valeurs d'une dimension
```

### Requêter

```bash
cube query infrastructures_surface --group-by Faculté
cube query infrastructures_surface --group-by Faculté --filter Faculté=FBM --filter Faculté=SSP
cube query infrastructures_surface --group-by Faculté --exclude Type=Labo --arrange indicateur:desc --limit 5
cube query infrastructures_surface --group-by Faculté --format json
```

### SQL brut

```bash
cube sql infrastructures_surface "SELECT Faculté, SUM(indicateur) FROM data GROUP BY Faculté"
```

### Synchroniser les cubes

```bash
cube sync             # Télécharger/mettre à jour depuis PROD
cube --dev sync       # Depuis l'environnement DEV
cube sync --force     # Forcer le re-téléchargement complet
```

## Cache local

| Environnement | Répertoire |
|---------------|------------|
| PROD | `~/.unisis-cube/cache/` |
| DEV | `~/.unisis-cube/cache-dev/` |
