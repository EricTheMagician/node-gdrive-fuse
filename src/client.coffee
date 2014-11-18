require("coffee-script")
google = require 'googleapis'
Fibers = require 'fibers'
Future = require 'fibers/future'
readline = require 'readline'
fs = require 'fs-extra'
winston = require 'winston'
rest = require 'restler'
hashmap = require( 'hashmap' ).HashMap
NodeCache = require 'node-cache'
pth = require 'path'
folder = require("./folder.coffee")
GFolder = folder.GFolder
GFile = require("./file.coffee").GFile
uploadTree = require("./folder.coffee").uploadTree
#read input config
config = fs.readJSONSync 'config.json'

#get logger
logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'info' }),
      new (winston.transports.File)({ filename: '/tmp/GDriveF4JS.log', level:'debug' })
    ]
  })


####################################
####### Client Variables ###########
####################################

#Create maps of name and files
folderTree = new hashmap()
now = (new Date).getTime()
folderTree.set('/', new GFolder(null, null, 'root',now, now,true, ['loading data']))
idToPath = new hashmap()

wait = Future.wait
exists = Future.wrap (path, cb) ->
  fs.exists path, (success)->
    cb(null,success)

OAuth2Client = google.auth.OAuth2
oauth2Client = new OAuth2Client(config.clientId, config.clientSecret, config.redirectUrl)
drive = google.drive({ version: 'v2' })

dataLocation = pth.join config.cacheLocation, 'data'
fs.ensureDirSync( dataLocation )

largestChangeId = 1;
####################################
####### Client Functions ###########
####################################


getPageFiles = Future.wrap (pageToken, cb) ->
  opts =
    fields: "etag,items(copyable,createdDate,downloadUrl,editable,fileExtension,fileSize,id,kind,labels(hidden,restricted,trashed),md5Checksum,mimeType,modifiedDate,parents(id,isRoot),shared,title,userPermission),nextPageToken"
    maxResults: 500
    pageToken: pageToken
  drive.files.list opts, (err, resp) ->
    if err
      logger.log 'error', err
      cb(err)

    data = {items:  resp.items }
    if resp.nextPageToken
      data.pageToken = resp.nextPageToken

    cb(null, data)

getAllFiles = Future.wrap (cb) ->
  items = []
  Fibers () ->
    data = getPageFiles(null).wait()
    items = items.concat(data.items)
    while data.pageToken
      data = getPageFiles(data.pageToken).wait()
      items = items.concat(data.items)
    getLargestChangeId().wait()
    cb(null, items)


  .run()

parseFilesFolders = (items) ->
  files = []
  folders = []
  for i in items
    if (!i.parents ) or i.parents.length == 0 
      continue
    unless i.labels.trashed
      if i.mimeType is "application/vnd.google-apps.folder"
        folders.push i
      else
        files.push i

  return {files:files, folders:folders}

loadFolderTree = ->
  jsonFile =  "#{config.cacheLocation}/data/folderTree.json"
  now = Date.now()

  fs.readJson jsonFile, (err, data) ->
    for key in Object.keys(data)
      o = data[key]

      #make sure parent directory exists
      unless folderTree.has(pth.dirname(key))
        continue

      #add to idToPath
      idToPath.set(o.id,key)
      idToPath.set(o.parentid, pth.basename(key))

      if o.size
        folderTree.set key, new GFile( o.downloadUrl, o.id, o.parentid, o.name, parseInt(o.size), new Date(o.ctime), new Date(o.mtime), o.permission )
      else
        # keep track of the conversion of bitcasa path to real path
        idToPath.set o.path, key
        folderTree.set key, new GFolder(o.id, o.parentid, o.name, new Date(o.ctime), new Date(o.mtime), o.permission, o.children)

    changeFile = "#{config.cacheLocation}/data/largestChangeId.json"
    fs.readJson changeFile, (err, data) ->
      largestChangeId = data.largestChangeId
      loadChanges()

lockFolderTree = false
saveFolderTree = () ->
  unless lockFolderTree
    lockFolderTree = true
    logger.debug "saving folder tree"
    toSave = {}
    for key in folderTree.keys()
      value = folderTree.get key
      toSave[key] = value

    fs.outputJson "#{config.cacheLocation}/data/folderTree.json", toSave,  ->
    lockFolderTree = false


getLargestChangeId = Future.wrap (cb)->
  opts =
    fields: "largestChangeId"
  callback = (err, res) ->
    unless err
      res.largestChangeId = parseInt(res.largestChangeId) + 1
      largestChangeId = res.largestChangeId
      fs.outputJsonSync "#{config.cacheLocation}/data/largestChangeId.json", res
    cb(err, res)
  drive.changes.list opts, callback

loadPageChange =  Future.wrap (pageToken, startChangeId, cb) ->

  opts =
    maxResults: 1000
    startChangeId:  startChangeId
    nextPageToken:  pageToken
    trashed:        false


  drive.changes.list opts, (err, res) ->
    unless err
      data =
        items: res.items
        largestChangeId: res.largestChangeId
        pageToken: res.nextPageToken
      cb(err, data)
    else
      cb(err)


loadChanges = Future.wrap (cb) ->
  id = largestChangeId
  items = []
  Fibers () ->
    data = loadPageChange(null,id).wait()
    items = items.concat data.items
    largestChangeId = data.largestChangeId

    while data.pageToken
      data = loadPageChange(data.pageToken,id).wait()
      items = items.concat data.items

    cb(null, items)
    if id != largestChangeId
      fs.outputJsonSync "#{config.cacheLocation}/data/largestChangeId.json", {largestChangeId:largestChangeId}

  .run()
####################################
###### Setting up the Client #######
####################################



scopes = [
  'https://www.googleapis.com/auth/drive',
]
Fibers () ->

  if not (config.accessToken)
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

  #create (or read) the folderTree
  if exists( pth.join(dataLocation, 'folderTree.json') ).wait()
    logger.log 'info', "Loading folder structure"
    loadFolderTree()
  else
    logger.log 'info', "Downloading full folder structure from google"
    items = getAllFiles().wait()

    logger.log 'info', "Finished downloading folder structure from google"
    logger.info "Parinsg data, looking for root foolder"

    data = parseFilesFolders items

    items = null # let node's garbage collection kick in

    now = (new Date).getTime()


    for f in data.folders
      #check to make sure that parents is defined.
      if (!f.parents ) or f.parents.length == 0 
        logger.log "debug", "folder.parents is undefined or empty"
        logger.log "debug", f
        continue

      if f.parents[0].isRoot
        folderTree.set('/', new GFolder(f.parents[0].id, null, 'root',now, now,true))
        idToPath.set(f.parents[0].id, '/')
        logger.log "info", "root node found"
        break

    # google does not return the list of files and folders in a particular order.
    # so parse folders first
    # and then parse files
    left = data.folders
    while left.length > 0
      logger.info "Folders left to parse: #{left.length}"
      notFound = []

      for f in data.folders
        # if (!f.parents ) or f.parents.length == 0 
        #   logger.log "debug", "folder.parents is undefined or empty"
        #   logger.log "debug", f
        #   continue
        pid = f.parents[0].id #parent id
        parentPath = idToPath.get(pid)
        if parentPath

          #if the parent exists, get it
          parent = folderTree.get(parentPath)
          path = pth.join(parentPath, f.title)
          idToPath.set( f.id, path)

          #push this current folder to the parent's children list
          if parent.children.indexOf(f.title) < 0
            parent.children.push f.title
          else
            continue
          #set up the new folder
          folderTree.set(path, new GFolder(f.id, pid, f.title, (new Date(f.createdDate)).getTime(), (new Date(f.modifiedDate)).getTime(), f.editable ))
        else
          notFound.push f
      left = notFound

    data.folders = null
    
    logger.info "Parsing files"
    for f in data.files
      pid = f.parents[0].id
      parentPath = idToPath.get(pid)
      if parentPath
        parent = folderTree.get parentPath
        if parent.children.indexOf(f.title) < 0
          parent.children.push f.title

        path = pth.join parentPath, f.title
        folderTree.set path, new GFile(f.downloadUrl, f.id, pid, f.title, parseInt(f.fileSize), (new Date(f.createdDate)).getTime(), (new Date(f.modifiedDate)).getTime(), f.editable)

    logger.info "Finished parsing files"
    logger.info "Everything should be ready to use"
    saveFolderTree()

  # loadChanges()
  # console.log getLargestChangeId().wait()
.run()

module.exports.folderTree = folderTree
module.exports.saveFolderTree = saveFolderTree
module.exports.drive = drive
module.exports.loadChanges = loadChanges
