# Trello CAD archive

Downloads every CAD / render / PDF / zip attachment from the Cullen Trello
workspace into a local folder, with a SQLite + CSV catalogue.

The repo includes a catalogue already indexed on 2026-11-19 (19 boards,
350k attachments; 46,075 CAD files ≈ 91 GB, 307k downloadable files ≈ 198 GB total),
so you can go straight to downloading. See `report.md` for the breakdown.

## Setup (once)

    pip install requests
    xz -dk cad_archive.sqlite.xz          # -> cad_archive.sqlite (~310 MB)
    export TRELLO_API_KEY=...             # from https://trello.com/power-ups/admin
    export TRELLO_TOKEN=...               # read-only token is enough

Windows without `xz`: open the `.xz` file with 7-Zip.

## Download

    python3 trello_cad.py download --categories cad --limit 25   # quick test
    python3 trello_cad.py download --categories cad              # all CAD (~91 GB)
    python3 trello_cad.py download                               # CAD + renders + PDF + zip (~198 GB)

Files land in `archive/<supplier or board>/<year>/<card>/<date>_<file>`.
Identical files are stored once (sha256); the catalogue points duplicates at
the original. It is safe to stop and restart: finished files are skipped.
Use `--retry-failed` to retry failures, `--board Christy` to limit to one board.
Check free disk space first. Set `CAD_OUT=/path/to/big/drive` to choose the folder.

## Other commands

    python3 trello_cad.py index          # re-index Trello (refreshes the catalogue)
    python3 trello_cad.py report         # rewrite report.md
    python3 trello_cad.py recategorize   # re-apply file-type rules
    python3 trello_cad.py export         # archive/catalogue.csv

## Sync to Google Drive

    rclone copy archive/ gdrive:Trello-CAD-Archive --progress
