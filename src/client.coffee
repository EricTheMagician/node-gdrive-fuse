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

now = Date.now()

#Create maps of name and files
inodeTree = new hashmap()
inodeTree.set(1,new GFolder(null, null, 'root',now, now, 1, true, []))
idToInode = new hashmap()
#idToPath = new hashmap()
#inodeToPath = new hashmap()
#inodeToPath.set 1, '/'

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

__items_to_parse_from_google__ = []

getPageFiles = (pageToken, total, cb) ->
  opts =
    fields: "etag,items(copyable,createdDate,downloadUrl,editable,fileExtension,fileSize,id,kind,labels(hidden,restricted,trashed),md5Checksum,mimeType,modifiedDate,parents(id,isRoot),shared,title,userPermission, version),nextPageToken"
    maxResults: 500
    pageToken: pageToken
  # logger.silly "current length of items during downloading of all files and folders is #{total} - left to parse: #{__items_to_parse_from_google__.length}"

  drive.files.list opts, (err, resp) ->
    if err
      logger.log 'error', "There was an error while downloading files from google, retrying"
      logger.error err
      fn = ->
        getPageFiles(pageToken, total, cb)
        return
      setTimeout(fn, 4000)
      return
    __items_to_parse_from_google__ = __items_to_parse_from_google__.concat(resp.items)
    newTotal = total + resp.items.length
    if newTotal > 10000
      newTotal -= 10000
      logger.info "Taking a break from downloading files to try and parse files and folders. Current items to parse: #{__items_to_parse_from_google__.length}"
      parseFilesFolders()
    cb(null, newTotal, resp.nextPageToken)
    return
  return

getAllFiles = ()->
  callback = (err, total, nextPageToken) ->
    logger.debug "current length of items during downloading of all files is #{__items_to_parse_from_google__.length}"
    if nextPageToken
      getPageFiles(nextPageToken, total, callback)
    else
      logger.log 'info', "Finished downloading folder structure from google"
      getLargestChangeId()
      parseFilesFolders()
      logger.debug __items_to_parse_from_google__
      saveFolderTree()
      getLargestChangeId()
      if require.main != module
        setTimeout loadChanges, 90000

    return
  getPageFiles(null, 0, callback)
  return

parseFilesFolders = () ->
  items = __items_to_parse_from_google__
  __items_to_parse_from_google__ = []
  logger.debug "Starting to parse items from google."
  logger.debug "There are #{items.length}  items to parse and the current inodeTree size is #{inodeTree.count()}."
  files = []
  folders = []
  root = inodeTree.get(1)
  if root.id
    rootFound = true
  else
    rootFound = false
  now = (new Date).getTime()

  inodes = value.inode for value in inodeTree.values()
  inodeCount = Math.max(inodes) + 1

  logger.info "Parinsg data, looking for root foolder"
  # google does not return the list of files and folders in a particular order.
  # so find the root folder first,
  # then parse the folders
  # and then parse files

  fs.outputJsonSync "#{config.cacheLocation}/data/unparsed.json", items


  for i in items
    if (! (i.parents) ) or i.parents.length == 0
      continue
    unless i.labels.trashed
      if i.deleted or i.labels.trashed
        continue

      if i.mimeType == "application/vnd.google-apps.folder"
        unless rootFound
          if i.parents[0].isRoot
            inodeTree.set(1, new GFolder(i.parents[0].id, null, 'root',now, now,1, true))
            idToInode.set(i.parents[0].id, 1)
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
      parentInode = idToInode.get(pid)
      if parentInode

        #if the parent exists, get it
        parent = inodeTree.get(parentInode)

        #check to see if parent is a folder
        if parent and parent instanceof GFolder
          unless parent.hasOwnProperty( "children")
            parent.children = []    
        else
          notFound.push f
          continue

        #check to see if id has already been set
        if idToInode.has(f.id)
          continue

        idToInode.set( f.id, inodeCount)

        #push this current folder to the parent's children list
        if parent.children.indexOf(inodeCount) < 0
          parent.children.push inodeCount
          inodeTree.set(inodeCount, new GFolder(f.id, pid, f.title, (new Date(f.createdDate)).getTime(), (new Date(f.modifiedDate)).getTime(), inodeCount, f.editable , []))
        inodeCount++          
      else
        notFound.push f

      #make sure that the folder list is gettting smaller over time.

    if left.length == notFound.length
      logger.info "There was #{left.length} folders that were not possible to process"
      # logger.debug notFound
      break
    left = notFound

  logger.info "Parsing files"
  for f in files
    pid = f.parents[0].id
    parentInode = idToInode.get(pid)
    if parentInode
      parent = inodeTree.get parentInode
      unless parent.children
        continue

      #add file to parent list
      parent.children.push inodeCount

      idToInode.set( f.id, inodeCount)
      inodeTree.set inodeCount, new GFile(f.downloadUrl, f.id, pid, f.title, parseInt(f.fileSize), (new Date(f.createdDate)).getTime(), (new Date(f.modifiedDate)).getTime(), inodeCount, f.editable)
      inodeCount++
    else
      left.push(f)

  __items_to_parse_from_google__ = __items_to_parse_from_google__.concat(left)

  # logger.info "Finished parsing files"
  # logger.info "Everything should be ready to use"
  # saveFolderTree()
  logger.debug "After attempting to parse, there is #{inodeTree.count()} items in the inodeTree and #{__items_to_parse_from_google__.length} items that were not yet parseable"
  return

parseFolderTreeInode = ->
  jsonFile =  "#{config.cacheLocation}/data/inodeTree.json"  
  now = Date.now()
  fs.readJson jsonFile, (err, data) ->
    try
      for key in Object.keys(data)
        o = data[key]
        inode = o.inode

        #add to idToPath
        idToPath.set(o.id,key)
        idToPath.set(o.parentid, pth.dirname(key))

        if 'size' of o
          inodeTree.set key, new GFile( o.downloadUrl, o.id, o.parentid, o.name, o.size, o.ctime, o.mtime, o.inode, o.permission )
        else
          inodeTree.set key, new GFolder(o.id, o.parentid, o.name, o.ctime, o.mtime, o.inode, o.permission,o.children)

      changeFile = "#{config.cacheLocation}/data/largestChangeId.json"
      fs.exists changeFile, (exists) ->
        if exists
          fs.readJson changeFile, (err, data) ->
            largestChangeId = data.largestChangeId
            if require.main != module
              loadChanges()
        return
    catch error
      #if there was an error with reading the file, just download the whole structure again
      getAllFiles()
    return
  return

parseFolderTree = ->
  jsonFile =  "#{config.cacheLocation}/data/inodeTree.json"
  now = Date.now()
  fs.readJson jsonFile, (err, data) ->
    try
      for key in Object.keys(data)
        o = data[key]
        if key == "1"
          inodeTree.set 1, new GFolder(o.id, o.parentid, o.name, o.ctime, o.mitime, o.inode, o.permission, o.children)
          idToInode.set o.id, 1
          continue

        #make sure parent directory exists
        unless idToInode.has( o.parentid )
          console.log o
          break
          continue

        idToInode.set o.id, o.inode

        if 'size' of o
          inodeTree.set o.inode, new GFile( o.downloadUrl, o.id, o.parentid, o.name, o.size, o.ctime, o.mtime, o.inode, o.permission )
        else
          inodeTree.set o.inode, new GFolder(o.id, o.parentid, o.name, o.ctime, o.mtime, o.inode, o.permission,o.children)

      changeFile = "#{config.cacheLocation}/data/largestChangeId.json"
      fs.exists changeFile, (exists) ->
        if exists
          fs.readJson changeFile, (err, data) ->
            if err
              largestChangeId = 0
            else
              largestChangeId = data.largestChangeId
            if require.main != module
              loadChanges()
        return
    catch error
      #if there was an error with reading the file, just download the whole structure again
      logger.debug error
      getAllFiles()
    return
  return

loadFolderTree = ->
  #create (or read) the folderTree
  fs.exists pth.join(dataLocation, 'inodeTree.json'), (exists) ->
    logger.debug "Folder tree exist status: #{exists}"
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
    for key in inodeTree.keys()
      value = inodeTree.get key
      toSave[key] = value

    fs.outputJson "#{config.cacheLocation}/data/inodeTree.json", toSave,  ->
      return
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
  logger.log items
  for i in items

    if i.deleted or i.file.labels.trashed #check if it is deleted
      if idToInode.has i.fileId #check to see if the file was not already removed from folderTree
        logger.debug "#{i.file.title} was deleted"
        id = i.fileId
        inode = idToInode.get id
        obj = inodeTree.get inode
        inodeTree.remove inode
        idToInode.remove id

        parent = inodeTree.get obj.parentid
        unless parent
          continue
        idx = parent.children.indexOf inode
        if idx >= 0
          parent.children.splice(idx, 1)
      else
        try
          logger.debug "processing a file that was marked as deleted, but not preset in the inodeTree: #{i.file.title} with id #{i.file.id}"
        catch e
          logger.debug "processfile a file that was marked as deleted but not present in the inodeTree"
          logger.debug i
      continue

    cfile = i.file #changed file
    unless cfile
      continue


    #if it is not deleted or trashed, check to see if it's new or not
    inode = idToInode.get(cfile.id)
    if inode
      f = inodeTree.get(inode)
      logger.debug "#{f.name} was updated"

      unless f
        idToPath.remove path
        notFound.push i
        continue
      f.ctime = (new Date(cfile.createdDate)).getTime()
      f.mtime = (new Date(cfile.modifiedDate)).getTime()
      if  f.name != cfile.title
        logger.info "#{f.name} was renamed to #{cfile.title}"
        f.name = cfile.title
      if f.parentid != cfile.parents[0].id
        logger.info "#{f.name} has moved"
        f.parentid = cfile.parents[0].id
      if f instanceof GFile
        f.downloadUrl = cfile.downloadUrl
      continue


    if cfile == undefined or cfile.parents == undefined or cfile.parents[0] == undefined
      logger.debug "changed file had empty parents"
      logger.debug cfile
      continue

    parentId = cfile.parents[0].id
    parentInode = idToInode.get(parentId)
    unless parentInode
      notFound.push i
      continue
    parent = inodeTree.get parentInode
    inodes = value.inode for value in inodeTree.values()
    inode = Math.max(inodes) + 1
    idToInode.set cfile.id, inode
    parent.children.push inode
    if cfile.mimeType == 'application/vnd.google-apps.folder'
      logger.debug "#{cfile.title} is a new folder"
      inodeTree.set inode, new GFolder(cfile.id, parentId, cfile.title, (new Date(cfile.createdDate)).getTime(), (new Date(cfile.modifiedDate)).getTime(), inode, cfile.editable )
    else
      logger.debug "#{cfile.title} is a new file"
      inodeTree.set inode, new GFile(cfile.downloadUrl, cfile.id, parentId, cfile.title, parseInt(cfile.fileSize), (new Date(cfile.createdDate)).getTime(), (new Date(cfile.modifiedDate)).getTime(),inode, cfile.editable)
    
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

module.exports.idToInode = idToInode
module.exports.inodeTree = inodeTree
module.exports.saveFolderTree = saveFolderTree
module.exports.drive = drive
module.exports.loadChanges = loadChanges
module.exports.parseFilesFolders = parseFilesFolders

