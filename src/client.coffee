google = require 'googleapis'
Fibers = require 'fibers'
Future = require 'fibers/future'
readline = require 'readline'
fs = require 'fs-extra'
winston = require 'winston'
rest = require 'restler'

#wait
wait = Future.wait

#read input config
config = fs.readJSONSync 'config.json'

#get logger
logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'info' }),
      new (winston.transports.File)({ filename: '/tmp/GDriveF4JS.log', level:'debug' })
    ]
  })

OAuth2Client = google.auth.OAuth2
oauth2Client = new OAuth2Client(config.clientId, config.clientSecret, config.redirectUrl)
drive = google.drive({ version: 'v2' })

####################################
####### Client Functions ###########
####################################

getPageFiles = Future.wrap (pageToken, cb) ->
  drive.files.list { pageToken: pageToken, trashed: false, maxResults:500}, (err, resp) ->
    if err
      logger.log 'error', err
      cb(err)

    data = {items:  resp.items }
    if resp.nextPageToken
      data.pageToken = resp.nextPageToken

    cb(null, data)

getAllFiles = Future.wrap (cb) ->
  items = [];
  Fibers () ->
    count = 1
    data = getPageFiles(null).wait()
    items = items.concat(data.items)
    while data.pageToken
      data = getPageFiles(data.pageToken).wait()
      items = items.concat(data.items)
      count++
      console.log "number of data fetches #{count} - items length: #{items.length}"

    console.log items.length
    for i in items
      console.log i

    cb(null, items)


  .run()





scopes = [
  'https://www.googleapis.com/auth/drive',
]
Fibers () ->

  if not config.accessToken
    url = oauth2Client.generateAuthUrl
      access_type: 'offline', # 'online' (default) or 'offline' (gets refresh_token)
      scope: scopes #If you only need one scope you can pass it as string
      approval_prompt: 'force' #Force user ti reapprove to get the refresh_token
    console.log url

    # create interface to read access code
    rl = readline.createInterface
      input: process.stdin,
      output: process.stdout

    _getToken = (cb) ->
      rl.question 'Enter the code here:', (code) ->
        # request access token
        oauth2Client.getToken code, (err,tokens) ->
          oauth2Client.setCredentials(tokens)
          config.accessToken = tokens
          fs.outputJsonSync 'config.json', config
          cb(null,true)

    getToken = Future.wrap _getToken
    getToken().wait()

    rl.close()
  else
    oauth2Client.setCredentials config.accessToken
  google.options({ auth: oauth2Client, user: config.email })

  console.log "Access Token Set"

  items = getAllFiles().wait()




.run()
