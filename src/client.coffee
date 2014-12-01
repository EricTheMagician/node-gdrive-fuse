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
config = fs.readJSONSync 'config.json'

#get logger
logger =  f.logger


####################################
####### Client Variables ###########
####################################

#Create maps of name and files
folderTree = new hashmap()
now = (new Date).getTime()
folderTree.set('/', new GFolder(null, null, 'root',now, now,true, ['loading data']))
idToPath = new hashmap()


OAuth2Client = google.auth.OAuth2
oauth2Client = new OAuth2Client(config.clientId, config.clientSecret, config.redirectUrl)
drive = google.drive({ version: 'v2' })

dataLocation = pth.join config.cacheLocation, 'data'
fs.ensureDirSync( dataLocation )

largestChangeId = 1;
####################################
####### Client Functions ###########
####################################


getPageFiles = (pageToken, items, cb) ->
  opts =
    fields: "etag,items(copyable,createdDate,downloadUrl,editable,fileExtension,fileSize,id,kind,labels(hidden,restricted,trashed),md5Checksum,mimeType,modifiedDate,parents(id,isRoot),shared,title,userPermission),nextPageToken"
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
            folderTree.set('/', new GFolder(i.parents[0].id, null, 'root',now, now,true))
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
        folderTree.set(path, new GFolder(f.id, pid, f.title, (new Date(f.createdDate)).getTime(), (new Date(f.modifiedDate)).getTime(), f.editable ))
      else
        notFound.push f

      #make sure that the folder list is gettting smaller over time. 
    if left.length == notFound.length 
      logger.info "There #{left.length} folders that were not possible to process"
      break
    left = notFound
  
  logger.info "Parsing files"
  for f in files
    pid = f.parents[0].id
    parentPath = idToPath.get(pid)
    if parentPath
      parent = folderTree.get parentPath
      if parent.children.indexOf(f.title) < 0
        parent.children.push f.title

      path = pth.join parentPath, f.title
      idToPath.set( f.id, path)

      folderTree.set path, new GFile(f.downloadUrl, f.id, pid, f.title, parseInt(f.fileSize), (new Date(f.createdDate)).getTime(), (new Date(f.modifiedDate)).getTime(), f.editable)

  logger.info "Finished parsing files"
  logger.info "Everything should be ready to use"
  saveFolderTree()
  getLargestChangeId()
  setTimeout loadChanges, 90000
  return


parseFolderTree = ->
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
      idToPath.set(o.parentid, pth.dirname(key))

      if o.size >= 0
        folderTree.set key, new GFile( o.downloadUrl, o.id, o.parentid, o.name, o.size, new Date(o.ctime), new Date(o.mtime), o.permission )
      else
        # keep track of the conversion of bitcasa path to real path
        idToPath.set o.path, key
        folderTree.set key, new GFolder(o.id, o.parentid, o.name, new Date(o.ctime), new Date(o.mtime), o.permission, o.children)

    changeFile = "#{config.cacheLocation}/data/largestChangeId.json"
    fs.readJson changeFile, (err, data) ->
      largestChangeId = data.largestChangeId
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
  id = largestChangeId+1
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
  for i in items

    if i.deleted #check if it is deleted
      path = idToPath.get(i.fileId)      
      if folderTree.has path #check to see if the file was not already removed from folderTree
        logger.debug "#{path} was deleted"
        folderTree.remove path
        parent = folderTree.get pth.dirname(path)
        idx = parent.children.indexOf pth.basename(path)
        if idx >= 0
          parent.children.splice(idx, 1)
        idToPath.remove i.fileId
      continue
    else
      cfile = i.file      

      # if it is not deleted, check to see if it's been marked as trash
      if cfile.labels.trashed
        if folderTree.has path
          folderTree.remove path
          parent = folderTree.get pth.dirname(path)
          idx = parent.children.indexOf pth.basename(path)
          if idx >= 0
            parent.children.splice(idx, 1)
          idToPath.remove i.fileId
        continue


      #if it is not deleted or trashed, check to see if it's new or not
      if idToPath.has(i.fileId)
        logger.debug "#{path} was updated"
        path = idToPath.get(i.fileId)
        f = folderTree.get(path)
        f.ctime = (new Date(cfile.createdDate)).getTime()
        f.mtime = (new Date(cfile.modifiedDate)).getTime()
        if f instanceof GFile
          if cfile.downloadUrl
            f.downloadUrl = cfile.downloadUrl
        continue

      else
        parentId = cfile.parents[0].id
        parentPath = idToPath.get(parentId)
        parent = folderTree.get parentPath
        path = pth.join parentPath, cfile.title
        idToPath.set cfile.id, path
        if cfile.mimeType == 'application/vnd.google-apps.folder'
          logger.debug "#{path} is a new folder"          
          folderTree.set path, new GFolder(cfile.id, parentId, cfile.title, (new Date(cfile.createdDate)).getTime(), (new Date(cfile.modifiedDate)).getTime(), cfile.editable )
        else
          logger.debug "#{path} is a new file"
          folderTree.set path, new GFile(cfile.downloadUrl, cfile.id, parentId, cfile.title, parseInt(cfile.fileSize), (new Date(cfile.createdDate)).getTime(), (new Date(cfile.modifiedDate)).getTime(), cfile.editable)

  if items.length > 0
    fs.outputJsonSync "#{config.cacheLocation}/data/largestChangeId.json", {largestChangeId: largestChangeId}      
    saveFolderTree()

  setTimeout loadChanges, 60000 + Math.random() * (15000)
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
      console.log "Access Token Set"
      loadFolderTree()

      fs.outputJsonSync 'config.json', config
      return

    rl.close()
    return


else
  oauth2Client.setCredentials config.accessToken
  console.log "Access Token Set"
  loadFolderTree()

google.options({ auth: oauth2Client, user: config.email })
GFile.oauth = oauth2Client;
GFolder.oauth = oauth2Client;

module.exports.folderTree = folderTree
module.exports.idToPath = idToPath
module.exports.saveFolderTree = saveFolderTree
module.exports.drive = drive
module.exports.loadChanges = loadChanges
