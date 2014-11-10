google = require 'googleapis'
Fibers = require 'fibers'
Future = require 'fibers/future'
readline = require 'readline'
fs = require 'fs-extra'

# create interface to read access code
rl = readline.createInterface
  input: process.stdin,
  output: process.stdout


#read input config
config = fs.readJSONSync 'config.json'


OAuth2Client = google.auth.OAuth2
oauth2Client = new OAuth2Client(config.clientId, config.clientSecret, config.redirectUrl)

scopes = [
  'https://www.googleapis.com/auth/drive',
]

url = oauth2Client.generateAuthUrl
  access_type: 'offline', # 'online' (default) or 'offline' (gets refresh_token)
  scope: scopes #If you only need one scope you can pass it as string

if not config.accessToken
  Fibers  ->

      console.log "Please visit this link and enter the access code as input"
      console.log url

      _getAccessToken = (cb) ->
        #read access code
        rl.question 'Enter the code here:', (code) ->
          # request access token
          oauth2Client.getToken( code,cb )
          return null
      getAccessToken = Future.wrap _getAccessToken

      tokens = getAccessToken().wait()

      config.accessToken = tokens
      fs.outputJsonSync 'config.json', config
  .run()

oauth2Client.setCredentials(config.accessTokens)
console.log "Access Token Set"
