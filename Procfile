web: gunicorn app:app --workers 1 --threads 24 --timeout 300 --bind 0.0.0.0:$PORT
worker: python bot.py
