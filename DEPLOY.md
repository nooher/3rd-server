# Deploy 3rd Signal Server — Free in 3 minutes

## Railway (easiest — recommended)

1. Go to https://railway.app — free account
2. "New Project" → "Deploy from GitHub repo"
   OR drag the /server folder
3. Set start command: `node index.js`
4. Railway gives you a URL like: https://3rd-signal.up.railway.app
5. Copy that URL

## Update the app with your URL

Open these two files and replace the SIGNAL_SERVER:

`src/hooks/useMessaging.ts` line 7:
```
const SIGNAL_SERVER = 'https://YOUR-URL.up.railway.app';
```

`src/hooks/useWebRTC.ts` line 8:
```
const SIGNAL_SERVER = 'https://YOUR-URL.up.railway.app';
```

Then rebuild the app.

## Render (alternative)

1. https://render.com → New Web Service
2. Connect /server folder
3. Build: `npm install`
4. Start: `node index.js`
5. Free tier — sleeps after 15min inactivity (wakes on first message)

## Test locally first

```bash
cd server
npm install
node index.js
# Server running on :3001

# Then in useMessaging.ts and useWebRTC.ts:
# const SIGNAL_SERVER = 'http://YOUR-PC-IP:3001';
# (find your IP: ipconfig on Windows, ifconfig on Mac/Linux)
```
