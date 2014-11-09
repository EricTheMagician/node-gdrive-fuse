google = require 'googleapis'
OAuth2 = google.auth.OAuth2

fs = require 'fs-extra'

config = fs.readJSONSync 'config.json'
oauth2Client = new OAuth2(config.clientId, config.clientSecret, config.redirectUrl);

scope = [
  'https://www.googleapis.com/auth/drive',
]

url = oauth2Client.generateAuthUrl({
  access_type: 'offline', # 'online' (default) or 'offline' (gets refresh_token)
  scope: scopes #If you only need one scope you can pass it as string
});
