---
name: cube
description: >
  Interroger les données du datawarehouse UNISIS (Université de Lausanne) via le CLI `cube`.
  Utiliser ce skill dès que l'utilisateur pose une question sur les données de l'UNIL
  (étudiants, infrastructures, surfaces, facultés, effectifs, indicateurs, etc.),
  mentionne « cube », « S3 » (statistiques en self-service), ou demande des chiffres
  issus du datawarehouse UNISIS. Utiliser aussi quand l'utilisateur veut synchroniser,
  explorer ou interroger des cubes SQLite UNISIS.
---

# cube — CLI de requêtage du datawarehouse UNISIS

Le CLI `cube` permet d'interroger des fichiers SQLite exportés depuis le datawarehouse
de l'Université de Lausanne.

## Principe fondamental

Le CLI est auto-documenté. Ne jamais deviner les noms de cubes, dimensions, valeurs
ou options : toujours les découvrir via `--help` et `schema` avant de construire
une requête.

## Workflow

### 1. Découvrir la syntaxe

```bash
cube --help
cube <commande> --help
```

Le `--help` de chaque commande documente toutes les options, la logique de filtrage
et donne des exemples. Toujours le consulter en premier.

### 2. Explorer les cubes disponibles

```bash
cube schema
```

Retourne le catalogue JSON de tous les cubes avec leur description, dimensions,
mesure et nombre de lignes. Si le cache est vide, exécuter `cube sync` d'abord.

### 3. Inspecter un cube

```bash
cube schema <nom_du_cube>
cube schema <nom_du_cube> <nom_dimension>
```

Le premier niveau donne le schéma complet (dimensions, cardinalités, valeurs ou
échantillons). Le second niveau liste toutes les valeurs d'une dimension.
Inspecter le schéma avant de requêter pour utiliser les noms exacts (accents,
parenthèses, espaces).

### 4. Requêter

Consulter `cube query --help` et `cube sql --help` pour la syntaxe complète.
Utiliser `--format json` quand le résultat doit être traité programmatiquement.
