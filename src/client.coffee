google = require 'googleapis'
readline = require 'readline'
fs = require 'fs-extra'
winston = require 'winston'
rest = require 'restler'
hashmap = require( 'hashmap' ).HashMap
pth = require 'path'
folder = require("./folder")
GFolder = folder.GFolder
f = require("./file")
GFile = f.GFile
uploadTree = folder.uploadTree

#read input config
if fs.existsSync 'config.json'
  config = fs.readJSONSync 'config.json'
else
  config = {}

#get logger
logger =  f.logger


####################################
####### Client Variables ###########
####################################

#Create maps of name and files
folderTree = new hashmap()
now = (new Date).getTime()
folderTree.set('/', new GFolder(null, null, 'root',now, now, 1, true, ['loading data']))

idToPath = new hashmap()
inodeToPath = new hashmap()
inodeToPath.set 1, '/'

OAuth2Client = google.auth.OAuth2
oauth2Client = new OAuth2Client(config.clientId || "520595891712-6n4r5q6runjds8m5t39rbeb6bpa3bf6h.apps.googleusercontent.com"  , config.clientSecret || "cNy6nr-immKnVIzlUsvKgSW8", config.redirectUrl || "urn:ietf:wg:oauth:2.0:oob")
drive = google.drive({ version: 'v2' })

config.cacheLocation ||=  "/tmp/cache" 
config.refreshDelay ||= 60000
dataLocation = pth.join( config.cacheLocation, 'data' )
fs.ensureDirSync( dataLocation )

largestChangeId = 1;
####################################
####### Client Functions ###########
####################################


getPageFiles = (pageToken, items, cb) ->
  opts =
    fields: "etag,items(copyable,createdDate,downloadUrl,editable,fileExtension,fileSize,id,kind,labels(hidden,restricted,trashed),md5Checksum,mimeType,modifiedDate,parents(id,isRoot),shared,title,userPermission, version),nextPageToken"
    maxResults: 500
    pageToken: pageToken
  drive.files.list opts, (err, resp) ->
    if err
      logger.log 'error', "There was an error while downloading files from google, retrying"
      logger.error err
      fn = ->
        getPageFiles(pageToken, items, cb)
        return
      setTimeout(fn, 4000)
      return

    cb(null, resp.nextPageToken, items.concat(resp.items))
    return
  return

getAllFiles = ()->
  callback = (err, nextPageToken, items) ->
    if nextPageToken
      getPageFiles(nextPageToken, items, callback)
    else
      logger.log 'info', "Finished downloading folder structure from google"
      getLargestChangeId()
      parseFilesFolders(items)
    return
  getPageFiles(null, [], callback)
  return

parseFilesFolders = (items) ->
  files = []
  folders = []
  rootFound = false  
  now = (new Date).getTime()
  inodeCount = 2
  logger.info "Parinsg data, looking for root foolder"
  # google does not return the list of files and folders in a particular order.
  # so find the root folder first,
  # then parse the folders
  # and then parse files

  for i in items
    if (! (i.parents) ) or i.parents.length == 0 
      continue
    unless i.labels.trashed      
      if i.mimeType == "application/vnd.google-apps.folder"
        unless rootFound
          if i.parents[0].isRoot
            folderTree.set('/', new GFolder(i.parents[0].id, null, 'root',now, now,1, true))
            idToPath.set(i.parents[0].id, '/')
            logger.log "info", "root node found"
            rootFound = true

        folders.push i
      else
        files.push i

  left = folders
  while left.length > 0
    logger.info "Folders left to parse: #{left.length}"
    notFound = []

    for f in folders
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
        inodeToPath.set inodeCount, path
        folderTree.set(path, new GFolder(f.id, pid, f.title, (new Date(f.createdDate)).getTime(), (new Date(f.modifiedDate)).getTime(), inodeCount, f.editable , []))
        inodeCount++
      else
        notFound.push f

      #make sure that the folder list is gettting smaller over time. 
    if left.length == notFound.length 
      logger.info "There was #{left.length} folders that were not possible to process"
      logger.debug notFound
      break
    left = notFound
  
  logger.info "Parsing files"
  for f in files
    pid = f.parents[0].id
    parentPath = idToPath.get(pid)
    if parentPath
      parent = folderTree.get parentPath
      unless parent.children
        continue
      if parent.children.indexOf(f.title) < 0
        parent.children.push f.title

      path = pth.join parentPath, f.title
      idToPath.set( f.id, path)
      inodeToPath.set inodeCount, path

      folderTree.set path, new GFile(f.downloadUrl, f.id, pid, f.title, parseInt(f.fileSize), (new Date(f.createdDate)).getTime(), (new Date(f.modifiedDate)).getTime(), inodeCount, f.editable)
      inodeCount++

  logger.info "Finished parsing files"
  logger.info "Everything should be ready to use"
  saveFolderTree()
  getLargestChangeId()
  if require.main != module
    setTimeout loadChanges, 90000
  return

parseFolderTree = ->
  jsonFile =  "#{config.cacheLocation}/data/folderTree.json"
  now = Date.now()
  inode = 1
  fs.readJson jsonFile, (err, data) ->
    for key in Object.keys(data)
      o = data[key]
      inodeToPath.set inode, key
      o.inode = inode
      inode++
      #make sure parent directory exists
      unless folderTree.has(pth.dirname(key))
        continue

      #add to idToPath
      idToPath.set(o.id,key)
      idToPath.set(o.parentid, pth.dirname(key))

      if 'size' of o
        folderTree.set key, new GFile( o.downloadUrl, o.id, o.parentid, o.name, o.size, o.ctime, o.mtime, o.inode, o.permission )
      else
        folderTree.set key, new GFolder(o.id, o.parentid, o.name, o.ctime, o.mtime, o.inode, o.permission,o.children)

    changeFile = "#{config.cacheLocation}/data/largestChangeId.json"
    fs.exists changeFile, (exists) ->
      if exists
        fs.readJson changeFile, (err, data) ->
          largestChangeId = data.largestChangeId
          if require.main != module
            loadChanges()
      return
    
    return
  return

loadFolderTree = ->
  #create (or read) the folderTree
  fs.exists pth.join(dataLocation, 'folderTree.json'), (exists) ->
    if exists 
      logger.log 'info', "Loading folder structure"
      parseFolderTree()
    else
      logger.log 'info', "Downloading full folder structure from google"
      getAllFiles()    
    return
  return


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
  return


getLargestChangeId = (cb)->
  opts =
    fields: "largestChangeId"
  callback = (err, res) ->
    unless err
      res.largestChangeId = parseInt(res.largestChangeId) + 1
      largestChangeId = res.largestChangeId
      fs.outputJsonSync "#{config.cacheLocation}/data/largestChangeId.json", res      
    if typeof(cb) == 'function'
      cb()
    return
  drive.changes.list opts, callback
  return

loadPageChange = (start, items, cb) ->

  opts =
    maxResults: 500
    startChangeId: start  

  drive.changes.list opts, (err, res) ->
    unless err
      cb(err, res.largestChangeId, items.concat(res.items), res.nextPageToken)
    else
      logger.debug "There was an error while loading changes"
      logger.debug err
      cb(err, largestChangeId, items, start)
    return
  return


loadChanges = (cb) ->
  id = largestChangeId
  logger.debug "Getting changes from Google Drive. The last change id was #{largestChangeId}."

  callback = (err, newId, items, pageToken) ->
    largestChangeId = newId
    if pageToken
      drive.changes.list 
      loadPageChange(pageToken, items, callback)
    else
      parseChanges(items)
    return

  loadPageChange(id, [], callback)

 
  return

parseChanges = (items) ->
  logger.debug "There was #{items.length} to parse"
  notFound = []
  for i in items
    path = idToPath.get(i.fileId)      
    if i.deleted or i.file.labels.trashed #check if it is deleted
      if folderTree.has path #check to see if the file was not already removed from folderTree
        logger.debug "#{path} was deleted"
        o = folderTree.get path
        folderTree.remove path
        inodeToPath.remove o.inode
        parent = folderTree.get pth.dirname(path)
        idx = parent.children.indexOf pth.basename(path)
        if idx >= 0
          parent.children.splice(idx, 1)
        idToPath.remove i.fileId
      continue
  
    cfile = i.file #changed file     
    unless cfile
      continue
     
    #if it is not deleted or trashed, check to see if it's new or not
    if path
      logger.debug "#{path} was updated"          
      f = folderTree.get(path)
      unless f
        idToPath.remove path
        notFound.push i
        continue
      f.ctime = (new Date(cfile.createdDate)).getTime()
      f.mtime = (new Date(cfile.modifiedDate)).getTime()
      if f instanceof GFile
        if cfile.downloadUrl
          f.downloadUrl = cfile.downloadUrl
      continue

  
    if cfile == undefined or cfile.parents == undefined or cfile.parents[0] == undefined 
      logger.debug "changed file had empty parents"
      logger.debug cfile
      continue

    parentId = cfile.parents[0].id
    parentPath = idToPath.get(parentId)
    unless parentPath
      notFound.push i
      continue
    parent = folderTree.get parentPath
    path = pth.join parentPath, cfile.title
    idToPath.set cfile.id, path
    inodes = value.inode for value in folderTree.values()
    inode = Math.max(inodes) + 1
    if cfile.mimeType == 'application/vnd.google-apps.folder'
      logger.debug "#{path} is a new folder"          
      folderTree.set path, new GFolder(cfile.id, parentId, cfile.title, (new Date(cfile.createdDate)).getTime(), (new Date(cfile.modifiedDate)).getTime(), inode, cfile.editable )
      inodeToPath.set cfile.id, path
    else
      logger.debug "#{path} is a new file"
      folderTree.set path, new GFile(cfile.downloadUrl, cfile.id, parentId, cfile.title, parseInt(cfile.fileSize), (new Date(cfile.createdDate)).getTime(), (new Date(cfile.modifiedDate)).getTime(),inode, cfile.editable)
    inodeToPath.set inode, path

  if notFound.length > 0 and notFound.length < items.length
    parseChanges(notFound)
    return

  if items.length > 0
    fs.outputJsonSync "#{config.cacheLocation}/data/largestChangeId.json", {largestChangeId: largestChangeId}      
    saveFolderTree()

  logger.debug "Finished parsing changes from google"
  setTimeout loadChanges, config.refreshDelay + Math.random() * (config.refreshDelay) * 0.25
  return
####################################
###### Setting up the Client #######
####################################



scopes = [
  'https://www.googleapis.com/auth/drive',
]

if not (config.accessToken)
  url = oauth2Client.generateAuthUrl
    access_type: 'offline', # 'online' (default) or 'offline' (gets refresh_token)
    scope: scopes #If you only need one scope you can pass it as string
    approval_prompt: 'force' #Force user to reapprove to get the refresh_token
  console.log url

  # create interface to read access code
  rl = readline.createInterface
    input: process.stdin,
    output: process.stdout

  rl.question 'Enter the code here:', (code) ->
    # request access token
    oauth2Client.getToken code, (err,tokens) ->
      oauth2Client.setCredentials(tokens)
      config.accessToken = tokens
      logger.info "Access Token Set"
      loadFolderTree()

      fs.outputJsonSync 'config.json', config
      return

    rl.close()
    return


else
  oauth2Client.setCredentials config.accessToken
  console.log "Access Token Set"
  loadFolderTree()

google.options({ auth: oauth2Client })
GFile.oauth = oauth2Client;
GFolder.oauth = oauth2Client;

module.exports.folderTree = folderTree
module.exports.idToPath = idToPath
module.exports.saveFolderTree = saveFolderTree
module.exports.drive = drive
module.exports.loadChanges = loadChanges
module.exports.inodeToPath = inodeToPath
