# uc-chart-backend
Backend for UntitledCharts Sonolus server

# ffmpeg
This requires `ffmpeg` and `ffprobe` to be installed and available on PATH. Used to detect and re-encode VBR MP3 files as CBR.

Ubuntu installation: `sudo apt install ffmpeg`

# S3/R2
This requires a S3/R2 instance to work.

# PSQL Cron
This requires the `postgresql-XX-cron` extension!

Ubuntu installation: `sudo apt install postgresql-XX-cron` (XX is psql version)
- Modify `sudo nano /etc/postgresql/XX/main/postgresql.conf` (find `cron.database_name = 'db'`, and `shared_preload_libraries='pg_cron'`)
- `sudo systemctl restart postgresql`
- `sudo -i -u postgres`
- `psql`
- `\c your db name`
- `CREATE EXTENSION pg_cron;`
- Create the scheduler! (see command in scripts/database_setup.py, possibly the last one)