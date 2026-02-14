# 🐔 Sabong Saga Bot Dashboard

A web dashboard to manage your Sabong Saga auto-battle bot.

## Features
- Multi-account support
- Start/Stop bots from the browser
- Live match log streaming
- Edit JWT, chickens, battle items, and toggles
- Automatic chicken rotation (primary + backups)
- Battle item priority with auto-fallback
- Password-protected access

## Quick Start (Local)

```bash
# Install dependencies
pip install -r requirements.txt

# Run the server
python server.py
```

Then open `http://localhost:8000` in your browser.

**Default password:** `sabong123` (change it after first login!)

## Deploy to Railway (Free)

1. Create a GitHub account (if you don't have one)
2. Create a new repository and push this code:
   ```bash
   git init
   git add .
   git commit -m "initial commit"
   git branch -M main
   git remote add origin https://github.com/YOUR_USERNAME/sabong-bot.git
   git push -u origin main
   ```

3. Go to [railway.app](https://railway.app) and sign up with GitHub
4. Click "New Project" → "Deploy from GitHub Repo"
5. Select your repository
6. Railway will auto-detect Python and deploy
7. Go to Settings → Networking → Generate Domain
8. Your app is live at `https://your-app.up.railway.app`

## Project Structure
```
sabong-web/
├── server.py          # Backend (FastAPI + Bot logic)
├── static/
│   └── index.html     # Frontend dashboard
├── requirements.txt   # Python dependencies
├── Procfile          # Railway process file
├── railway.toml      # Railway config
└── sabong.db         # SQLite database (created on first run)
```

## Changing Password
Default password is `sabong123`. Change it through the API:
```bash
curl -X POST http://localhost:8000/api/change-password \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"password": "your_new_password"}'
```

## Security Notes
- Always change the default password
- JWTs are stored server-side in SQLite
- All API endpoints require authentication
- Use HTTPS in production (Railway provides this automatically)
