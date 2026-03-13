# cube — UNISIS S3 Cubes CLI

CLI pour interroger les cubes SQLite de la plateforme **UNISIS S3** (Statistiques en Self-Service) de l'Université de Lausanne.

## Fonctionnalités

- **Catalogue et exploration** — Parcours progressif des cubes disponibles, avec recherche par regex insensible aux accents
- **Requêtes structurées** — Agrégation, filtrage, tri et limitation via des flags typés (pas besoin d'écrire du SQL)
- **SQL brut** — Pour les requêtes complexes impossibles à exprimer avec les flags
- **Synchronisation** — Téléchargement incrémental depuis Google Cloud Storage avec vérification CRC32C et intégrité SQLite

## Prérequis

- [Google Cloud SDK](https://cloud.google.com/sdk) (`gcloud`) — pour la synchronisation

Avant la première synchronisation :

```bash
gcloud auth application-default login
```

## Installation

### Homebrew (macOS / Linux)

```bash
brew tap unisis-unil/tools
brew install cube
```

### Script d'installation

```bash
curl -fsSL https://raw.githubusercontent.com/unisis-unil/cube-cli/main/install.sh | sh
```

### Depuis les sources

Nécessite [Rust](https://rustup.rs/) (édition 2021+) et [just](https://github.com/casey/just) :

```bash
just install      # Compile et installe cube dans ~/.cargo/bin/
```

## Développement

```bash
just              # Affiche toutes les tâches disponibles
just build        # Compile en mode debug
just release      # Compile en mode release optimisé
just test         # Lance tous les tests
just check        # Vérifie le code (clippy + fmt)
just fmt          # Formate le code
just bump patch   # Bump de version, commit, tag
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
